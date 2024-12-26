use crate::physical_planner::intervals::{ColInterval, ColIntervals};
use crate::physical_planner::joins::utils::symmetric_join_output_partitioning;
use crate::physical_planner::joins::utils::{
    estimate_join_statistics, BuildProbeJoinMetrics, OnceAsync, OnceFut,
};
use crate::session_context::Algorithm;
use ahash::RandomState;
use bio::data_structures::interval_tree as rust_bio;
use coitrees::{COITree, Interval};
use datafusion::arrow::array::{Array, AsArray, PrimitiveArray, PrimitiveBuilder, RecordBatch};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, UInt32Type};
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{
    internal_err, plan_err, project_schema, DataFusionError, JoinSide, JoinType, Result, Statistics,
};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::equivalence::{join_equivalence_properties, ProjectionMapping};
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::{Distribution, Partitioning, PhysicalExpr, PhysicalExprRef};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::common::can_project;
use datafusion::physical_plan::joins::utils::{
    adjust_right_output_partitioning, build_join_schema, check_join_is_valid, ColumnIndex,
    JoinFilter, JoinOn, JoinOnRef, StatefulStreamResult,
};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    handle_state, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
    ExecutionPlanProperties, PlanProperties,
};
use fnv::FnvHashMap;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::mem::size_of;
use std::sync::Arc;
use std::task::Poll;

type Position = usize;

#[derive(Debug)]
struct JoinLeftData {
    hash_map: IntervalJoinAlgorithm,
    batch: RecordBatch,
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl JoinLeftData {
    fn new(
        hash_map: IntervalJoinAlgorithm,
        batch: RecordBatch,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            hash_map,
            batch,
            reservation,
        }
    }
}

#[derive(Debug)]
pub struct IntervalJoinExec {
    /// left (build) side which gets hashed
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub right: Arc<dyn ExecutionPlan>,
    /// Set of equijoin columns from the relations: `(left_col, right_col)`
    pub on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// Columns that represent start/end of an interval
    pub intervals: ColIntervals,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The output schema for the join
    join_schema: SchemaRef,
    /// Future that consumes left input and builds the hash table
    left_fut: OnceAsync<JoinLeftData>,
    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Partitioning mode to use
    pub mode: PartitionMode,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// The projection indices of the columns in the output schema of join
    pub projection: Option<Vec<usize>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Null matching behavior: If `null_equals_null` is true, rows that have
    /// `null`s in both left and right equijoin columns will be matched.
    /// Otherwise, rows that have `null`s in the join columns will not be
    /// matched and thus will not appear in the output.
    pub null_equals_null: bool,

    cache: PlanProperties,

    algorithm: Algorithm,
}

impl IntervalJoinExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        intervals: ColIntervals,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
        partition_mode: PartitionMode,
        null_equals_null: bool,
        algorithm: Algorithm,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return plan_err!("On constraints in HashJoinExec should be non-empty");
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let join_schema = Arc::new(join_schema);

        //  check if the projection is valid
        can_project(&join_schema, projection.as_ref())?;

        let cache = Self::compute_properties(
            &left,
            &right,
            join_schema.clone(),
            *join_type,
            &on,
            partition_mode,
            projection.as_ref(),
        )?;

        Ok(IntervalJoinExec {
            left,
            right,
            on,
            filter,
            intervals,
            join_type: *join_type,
            join_schema,
            left_fut: Default::default(),
            random_state,
            mode: partition_mode,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
            column_indices,
            null_equals_null,
            cache,
            algorithm,
        })
    }

    /// left (build) side which gets hashed
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right (probe) side which are filtered by the hash table
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }

    /// Filters applied before join output
    pub fn filter(&self) -> Option<&JoinFilter> {
        self.filter.as_ref()
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// The partitioning mode of this hash join
    pub fn partition_mode(&self) -> &PartitionMode {
        &self.mode
    }

    /// Get null_equals_null
    pub fn null_equals_null(&self) -> bool {
        self.null_equals_null
    }

    /// Calculate order preservation flags for this hash join.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner | JoinType::RightAnti | JoinType::RightSemi
            ),
        ]
    }

    /// Get probe side information for the hash join.
    pub fn probe_side() -> JoinSide {
        // In current implementation right side is always probe side.
        JoinSide::Right
    }

    /// Return whether the join contains a projection
    pub fn contain_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// Return new instance of [HashJoinExec] with the given projection.
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        //  check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;
        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };
        Self::try_new(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            self.filter.clone(),
            self.intervals.clone(),
            &self.join_type,
            projection,
            self.mode,
            self.null_equals_null,
            self.algorithm,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
        on: JoinOnRef,
        mode: PartitionMode,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema.clone(),
            &Self::maintains_input_order(join_type),
            Some(Self::probe_side()),
            on,
        );

        // Get output partitioning:
        let left_columns_len = left.schema().fields.len();
        let mut output_partitioning = match mode {
            PartitionMode::CollectLeft => match join_type {
                JoinType::Inner | JoinType::Right => {
                    adjust_right_output_partitioning(right.output_partitioning(), left_columns_len)
                }
                JoinType::RightSemi | JoinType::RightAnti => right.output_partitioning().clone(),
                JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark
                | JoinType::Full => {
                    Partitioning::UnknownPartitioning(right.output_partitioning().partition_count())
                }
            },
            PartitionMode::Partitioned => {
                symmetric_join_output_partitioning(left, right, &join_type)
            }
            PartitionMode::Auto => {
                Partitioning::UnknownPartitioning(right.output_partitioning().partition_count())
            }
        };

        // Determine execution mode by checking whether this join is pipeline
        // breaking. This happens when the left side is unbounded, or the right
        // side is unbounded with `Left`, `Full`, `LeftAnti` or `LeftSemi` join types.
        let pipeline_breaking = left.execution_mode().is_unbounded()
            || (right.execution_mode().is_unbounded()
                && matches!(
                    join_type,
                    JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftSemi
                ));

        let mode = if pipeline_breaking {
            ExecutionMode::PipelineBreaking
        } else {
            crate::physical_planner::joins::utils::execution_mode_from_children([left, right])
        };

        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            // construct a map from the input expressions to the output expression of the Projection
            let projection_mapping = ProjectionMapping::from_indices(projection, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
            output_partitioning = output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            mode,
        ))
    }
}

impl DisplayAs for IntervalJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let display_projections = if self.contain_projection() {
                    format!(
                        ", projection=[{}]",
                        self.projection
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.join_schema.fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "IntervalJoinExec: mode={:?}, join_type={:?}, on=[{}]{}{}, alg={}",
                    self.mode,
                    self.join_type,
                    on,
                    display_filter,
                    display_projections,
                    self.algorithm
                )
            }
        }
    }
}

impl ExecutionPlan for IntervalJoinExec {
    fn name(&self) -> &'static str {
        "IntervalJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            PartitionMode::CollectLeft => vec![
                Distribution::SinglePartition,
                Distribution::UnspecifiedDistribution,
            ],
            PartitionMode::Partitioned => {
                let (left_expr, right_expr) =
                    self.on.iter().map(|(l, r)| (l.clone(), r.clone())).unzip();
                vec![
                    Distribution::HashPartitioned(left_expr),
                    Distribution::HashPartitioned(right_expr),
                ]
            }
            PartitionMode::Auto => vec![
                Distribution::UnspecifiedDistribution,
                Distribution::UnspecifiedDistribution,
            ],
        }
    }

    // For [JoinType::Inner] and [JoinType::RightSemi] in hash joins, the probe phase initiates by
    // applying the hash function to convert the join key(s) in each row into a hash value from the
    // probe side table in the order they're arranged. The hash value is used to look up corresponding
    // entries in the hash table that was constructed from the build side table during the build phase.
    //
    // Because of the immediate generation of result rows once a match is found,
    // the output of the join tends to follow the order in which the rows were read from
    // the probe side table. This is simply due to the sequence in which the rows were processed.
    // Hence, it appears that the hash join is preserving the order of the probe side.
    //
    // Meanwhile, in the case of a [JoinType::RightAnti] hash join,
    // the unmatched rows from the probe side are also kept in order.
    // This is because the **`RightAnti`** join is designed to return rows from the right
    // (probe side) table that have no match in the left (build side) table. Because the rows
    // are processed sequentially in the probe phase, and unmatched rows are directly output
    // as results, these results tend to retain the order of the probe side table.
    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IntervalJoinExec::try_new(
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.filter.clone(),
            self.intervals.clone(),
            &self.join_type,
            self.projection.clone(),
            self.mode,
            self.null_equals_null,
            self.algorithm,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let on_left = self.on.iter().map(|on| on.0.clone()).collect::<Vec<_>>();
        let on_right = self.on.iter().map(|on| on.1.clone()).collect::<Vec<_>>();
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();

        if self.mode == PartitionMode::Partitioned && left_partitions != right_partitions {
            return internal_err!(
                "Invalid IntervalSearchJoinExec, partition count mismatch {left_partitions}!={right_partitions},\
                 consider using RepartitionExec"
            );
        }

        let ColIntervals {
            left_interval,
            right_interval,
        } = self.intervals.clone();

        let join_metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);
        let left_fut = match self.mode {
            PartitionMode::CollectLeft => self.left_fut.once(|| {
                let reservation =
                    MemoryConsumer::new("HashJoinInput").register(context.memory_pool());
                collect_left_input(
                    None,
                    self.random_state.clone(),
                    self.left.clone(),
                    on_left.clone(),
                    context.clone(),
                    join_metrics.clone(),
                    reservation,
                    left_interval,
                    self.algorithm,
                )
            }),
            PartitionMode::Partitioned => {
                let reservation = MemoryConsumer::new(format!("HashJoinInput[{partition}]"))
                    .register(context.memory_pool());

                OnceFut::new(collect_left_input(
                    Some(partition),
                    self.random_state.clone(),
                    self.left.clone(),
                    on_left.clone(),
                    context.clone(),
                    join_metrics.clone(),
                    reservation,
                    left_interval,
                    self.algorithm,
                ))
            }
            PartitionMode::Auto => {
                return plan_err!(
                    "Invalid IntervalSearchJoinExec, unsupported PartitionMode {:?} in execute()",
                    PartitionMode::Auto
                );
            }
        };

        let reservation = MemoryConsumer::new(format!("HashJoinStream[{partition}]"))
            .register(context.memory_pool());

        // we have the batches and the hash map with their keys. We can how create a stream
        // over the right that uses this information to issue new batches.
        let right_stream = self.right.execute(partition, context.clone())?;

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => projection
                .iter()
                .map(|i| self.column_indices[*i].clone())
                .collect(),
            None => self.column_indices.clone(),
        };

        Ok(Box::pin(IntervalJoinStream {
            schema: self.schema(),
            on_left,
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            left_fut,
            right: right_stream,
            column_indices: column_indices_after_projection,
            random_state: self.random_state.clone(),
            join_metrics,
            null_equals_null: self.null_equals_null,
            reservation,
            right_interval,
            state: IntervalJoinStreamState::WaitBuildSide,
            build_side: None,
            hashes_buffer: vec![],
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        let stats = estimate_join_statistics(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.on.clone(),
            &self.join_type,
            &self.join_schema,
        )?;
        // Project statistics if there is a projection
        Ok(stats.project(self.projection.as_ref()))
    }
}

#[allow(clippy::too_many_arguments)]
async fn collect_left_input(
    partition: Option<usize>,
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<PhysicalExprRef>,
    context: Arc<TaskContext>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    left_interval: ColInterval,
    algorithm: Algorithm,
) -> Result<JoinLeftData> {
    let schema = left.schema();

    let (left_input, left_input_partition) = if let Some(partition) = partition {
        (left, partition)
    } else if left.output_partitioning().partition_count() != 1 {
        (Arc::new(CoalescePartitionsExec::new(left)) as _, 0)
    } else {
        (left, 0)
    };

    // Depending on partition argument load single partition or whole left side in memory
    let stream = left_input.execute(left_input_partition, context.clone())?;

    // This operation performs 2 steps at once:
    // 1. creates a [JoinHashMap] of all batches from the stream
    // 2. stores the batches in a vector.
    let initial = (Vec::new(), 0, metrics, reservation);
    let (batches, num_rows, metrics, mut reservation) = stream
        .try_fold(initial, |mut acc, batch| async {
            let batch_size = batch.get_array_memory_size();
            // Reserve memory for incoming batch
            acc.3.try_grow(batch_size)?;
            // Update metrics
            acc.2.build_mem_used.add(batch_size);
            acc.2.build_input_batches.add(1);
            acc.2.build_input_rows.add(batch.num_rows());
            // Update rowcount
            acc.1 += batch.num_rows();
            // Push batch to output
            acc.0.push(batch);
            Ok(acc)
        })
        .await?;

    // Estimation of memory size, required for hashtable, prior to allocation.
    // Final result can be verified using `RawTable.allocation_info()`
    //
    // For majority of cases hashbrown overestimates buckets qty to keep ~1/8 of them empty.
    // This formula leads to overallocation for small tables (< 8 elements) but fine overall.
    let estimated_buckets = (num_rows.checked_mul(8).ok_or_else(|| {
        DataFusionError::Execution(
            "usize overflow while estimating number of hasmap buckets".to_string(),
        )
    })? / 7)
        .next_power_of_two();
    // 16 bytes per `(u64, u64)`
    // + 1 byte for each bucket
    // + fixed size of JoinHashMap (RawTable + Vec)
    let estimated_hastable_size =
        16 * estimated_buckets + estimated_buckets + size_of::<IntervalJoinAlgorithm>();

    reservation.try_grow(estimated_hastable_size)?;
    metrics.build_mem_used.add(estimated_hastable_size);

    let mut hashmap = HashMap::<u64, Vec<SequilaInterval>>::new();
    let mut hashes_buffer = Vec::new();
    let mut offset = 0;

    // Updating hashmap starting from the last batch
    for batch in batches.iter() {
        // build a left hash map
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hashmap(
            &on_left,
            &left_interval,
            batch,
            &mut hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
        )?;
        offset += batch.num_rows();
    }

    let hashmap = IntervalJoinAlgorithm::new(&algorithm, hashmap);

    let single_batch = compute::concat_batches(&schema, &batches)?;
    let data = JoinLeftData::new(hashmap, single_batch, reservation);

    Ok(data)
}

struct SequilaInterval {
    start: i32,
    end: i32,
    position: Position,
}

impl SequilaInterval {
    fn new(start: i32, end: i32, position: Position) -> SequilaInterval {
        SequilaInterval {
            start,
            end,
            position,
        }
    }
    fn into_coitrees(self) -> coitrees::Interval<Position> {
        coitrees::Interval::new(self.start, self.end, self.position)
    }
    fn into_rust_bio(self) -> (std::ops::Range<i32>, Position) {
        (self.start..self.end + 1, self.position)
    }
    fn into_ailist(self) -> scailist::Interval<Position> {
        scailist::Interval {
            start: self.start as u32,
            end: self.end as u32 + 1,
            val: self.position,
        }
    }
    fn into_lapper(self) -> rust_lapper::Interval<u32, Position> {
        rust_lapper::Interval {
            start: self.start as u32,
            stop: self.end as u32 + 1,
            val: self.position,
        }
    }
}

enum IntervalJoinAlgorithm {
    Coitrees(FnvHashMap<u64, coitrees::COITree<Position, u32>>),
    IntervalTree(FnvHashMap<u64, rust_bio::IntervalTree<i32, Position>>),
    ArrayIntervalTree(FnvHashMap<u64, rust_bio::ArrayBackedIntervalTree<i32, Position>>),
    AIList(FnvHashMap<u64, scailist::ScAIList<Position>>),
    Lapper(FnvHashMap<u64, rust_lapper::Lapper<u32, Position>>),
    CoitresNearest(FnvHashMap<u64, (COITree<Position, u32>, Vec<Interval<Position>>)>),
}

impl Debug for IntervalJoinAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IntervalJoinAlgorithm::Coitrees(m) => {
                use coitrees::IntervalTree;
                let q = m
                    .iter()
                    .map(|(key, val)| (*key, val.iter().collect::<Vec<_>>()))
                    .collect::<HashMap<_, _>>();
                f.debug_struct("Coitrees").field("0", &q).finish()
            }
            &IntervalJoinAlgorithm::CoitresNearest(_) => todo!(),

            IntervalJoinAlgorithm::IntervalTree(m) => {
                f.debug_struct("IntervalTree").field("0", m).finish()
            }
            IntervalJoinAlgorithm::ArrayIntervalTree(m) => {
                f.debug_struct("ArrayIntervalTree").field("0", m).finish()
            }
            IntervalJoinAlgorithm::AIList(m) => f.debug_struct("AIList").field("0", m).finish(),
            IntervalJoinAlgorithm::Lapper(m) => f.debug_struct("Lapper").field("0", m).finish(),
        }
    }
}

impl IntervalJoinAlgorithm {
    fn new(alg: &Algorithm, hash_map: HashMap<u64, Vec<SequilaInterval>>) -> IntervalJoinAlgorithm {
        match alg {
            Algorithm::Coitrees => {
                use coitrees::{COITree, Interval, IntervalTree};

                let hashmap = hash_map
                    .into_iter()
                    .map(|(k, v)| {
                        let intervals = v
                            .into_iter()
                            .map(SequilaInterval::into_coitrees)
                            .collect::<Vec<Interval<Position>>>();

                        // can hold up to u32::MAX intervals
                        let tree: COITree<Position, u32> = COITree::new(intervals.iter());
                        (k, tree)
                    })
                    .collect::<FnvHashMap<u64, COITree<Position, u32>>>();

                IntervalJoinAlgorithm::Coitrees(hashmap)
            }
            Algorithm::CoitreesNearest => {
                use coitrees::{COITree, Interval, IntervalTree};

                let hashmap = hash_map
                    .into_iter()
                    .map(|(k, v)| {
                        let mut intervals = v
                            .into_iter()
                            .map(SequilaInterval::into_coitrees)
                            .collect::<Vec<Interval<Position>>>();

                        // can hold up to u32::MAX intervals
                        let tree: COITree<Position, u32> = COITree::new(intervals.iter());
                        intervals.sort_by(|a, b| {
                            a.first.cmp(&b.first).then_with(|| a.last.cmp(&b.last))
                        });
                        (k, (tree, intervals))
                    })
                    .collect::<FnvHashMap<u64, (COITree<Position, u32>, Vec<Interval<Position>>)>>(
                    );
                IntervalJoinAlgorithm::CoitresNearest(hashmap)
            }
            Algorithm::IntervalTree => {
                let hashmap = hash_map
                    .into_iter()
                    .map(|(k, v)| {
                        let tree = rust_bio::IntervalTree::from_iter(
                            v.into_iter().map(SequilaInterval::into_rust_bio),
                        );
                        (k, tree)
                    })
                    .collect::<FnvHashMap<u64, rust_bio::IntervalTree<i32, Position>>>();

                IntervalJoinAlgorithm::IntervalTree(hashmap)
            }
            Algorithm::ArrayIntervalTree => {
                let hashmap = hash_map
                    .into_iter()
                    .map(|(k, v)| {
                        let tree = rust_bio::ArrayBackedIntervalTree::<i32, Position>::from_iter(
                            v.into_iter().map(SequilaInterval::into_rust_bio),
                        );
                        (k, tree)
                    })
                    .collect::<FnvHashMap<u64, rust_bio::ArrayBackedIntervalTree<i32, Position>>>();

                IntervalJoinAlgorithm::ArrayIntervalTree(hashmap)
            }
            Algorithm::AIList => {
                let hashmap = hash_map
                    .into_iter()
                    .map(|(k, v)| {
                        let intervals = v
                            .into_iter()
                            .map(SequilaInterval::into_ailist)
                            .collect::<Vec<scailist::Interval<Position>>>();

                        (k, scailist::ScAIList::new(intervals, None))
                    })
                    .collect::<FnvHashMap<u64, scailist::ScAIList<Position>>>();

                IntervalJoinAlgorithm::AIList(hashmap)
            }
            Algorithm::Lapper => {
                use rust_lapper::*;
                let hashmap = hash_map
                    .into_iter()
                    .map(|(k, v)| {
                        let intervals = v
                            .into_iter()
                            .map(SequilaInterval::into_lapper)
                            .collect::<Vec<Interval<u32, Position>>>();

                        (k, Lapper::new(intervals))
                    })
                    .collect::<FnvHashMap<u64, Lapper<u32, Position>>>();

                IntervalJoinAlgorithm::Lapper(hashmap)
            }
        }
    }

    /// unoptimized on Linux x64 (without target-cpu=native)
    #[cfg(any(
        all(
            target_os = "linux",
            target_arch = "x86_64",
            not(target_feature = "avx")
        ),
        all(
            target_os = "macos",
            target_arch = "x86_64",
            not(target_feature = "avx")
        ),
        all(
            target_os = "windows",
            target_arch = "x86_64",
            not(target_feature = "avx")
        ),
    ))]
    fn extract_position(&self, node: &coitrees::IntervalNode<Position, u32>) -> Position {
        node.metadata
    }

    /// for Apple Intel, Apple M1+(both optimized and not) and optimized (target-cpu=native) on Linux x64
    #[cfg(any(
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64", target_feature = "avx"),
        all(target_os = "linux", target_arch = "x86_64", target_feature = "avx"),
        all(target_os = "windows", target_arch = "x86_64", target_feature = "avx")
    ))]
    fn extract_position(&self, node: &coitrees::Interval<&Position>) -> Position {
        *node.metadata
    }

    fn nearest(&self, start: i32, end: i32, ranges2: &[Interval<Position>]) -> Option<Position> {
        if ranges2.is_empty() {
            return None;
        }

        let sorted_ranges2 = ranges2;

        let mut closest_idx = None;
        let mut min_distance = i32::MAX;

        let mut left = 0;
        let mut right = sorted_ranges2.len();

        // Binary search to narrow down candidates in ranges2
        while left < right {
            let mid = (left + right) / 2;
            if sorted_ranges2[mid].first < end {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Check ranges around the binary search result for nearest distance
        for &i in [left.saturating_sub(1), left].iter() {
            if let Some(r2) = sorted_ranges2.get(i) {
                let distance = if end < r2.first {
                    r2.first - end
                } else if r2.last < start {
                    start - r2.last
                } else {
                    0
                };

                if distance < min_distance {
                    min_distance = distance;
                    closest_idx = Some(r2.metadata);
                }
            }
        }

        closest_idx
    }
    fn get<F>(&self, k: u64, start: i32, end: i32, mut f: F)
    where
        F: FnMut(Position),
    {
        match self {
            IntervalJoinAlgorithm::Coitrees(hashmap) => {
                use coitrees::IntervalTree;
                if let Some(tree) = hashmap.get(&k) {
                    tree.query(start, end, |node| {
                        let position: Position = self.extract_position(node);
                        f(position)
                    });
                }
            }
            IntervalJoinAlgorithm::CoitresNearest(hashmap) => {
                use coitrees::IntervalTree;
                if let Some(tree) = hashmap.get(&k) {
                    let mut i = 0;
                    // first look for overlaps and return an arbitrary one (see: https://web.mit.edu/~r/current/arch/i386_linux26/lib/R/library/IRanges/html/nearest-methods.html)
                    tree.0.query(start, end, |node| {
                        let position: Position = self.extract_position(node);
                        if i == 0 {
                            f(position);
                            i += 1;
                        }
                    });
                    // found no overlaps in the tree - try to look for nearest intervals
                    if i == 0 {
                        let position = self.nearest(start, end, &tree.1);
                        f(position.unwrap());
                    }
                }
            }
            IntervalJoinAlgorithm::IntervalTree(hashmap) => {
                if let Some(tree) = hashmap.get(&k) {
                    for entry in tree.find(start..end + 1) {
                        f(*entry.data())
                    }
                }
            }
            IntervalJoinAlgorithm::ArrayIntervalTree(hashmap) => {
                if let Some(tree) = hashmap.get(&k) {
                    for entry in tree.find(start..end + 1) {
                        f(*entry.data())
                    }
                }
            }
            IntervalJoinAlgorithm::AIList(hashmap) => {
                if let Some(list) = hashmap.get(&k) {
                    for interval in list.find(start as u32, end as u32 + 1) {
                        f(interval.val)
                    }
                }
            }
            IntervalJoinAlgorithm::Lapper(hashmap) => {
                if let Some(lapper) = hashmap.get(&k) {
                    for interval in lapper.find(start as u32, end as u32 + 1) {
                        f(interval.val)
                    }
                }
            }
        }
    }
}

fn update_hashmap(
    on: &[PhysicalExprRef],
    left_interval: &ColInterval,
    batch: &RecordBatch,
    hash_map: &mut HashMap<u64, Vec<SequilaInterval>>,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
) -> Result<()> {
    let keys_values = on
        .iter()
        .map(|c| c.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    let hash_values: &mut Vec<u64> = create_hashes(&keys_values, random_state, hashes_buffer)?;

    let hash_values_iter: Vec<(usize, &u64)> = hash_values
        .iter()
        .enumerate()
        .map(|(i, val)| (i + offset, val))
        .collect::<Vec<_>>();

    let start = evaluate_as_i32(left_interval.start(), batch)?;
    let end = evaluate_as_i32(left_interval.end(), batch)?;

    hash_values_iter
        .into_iter()
        .enumerate()
        .take(batch.num_rows())
        .for_each(|(i, (position, hash_val))| {
            let intervals: &mut Vec<SequilaInterval> = hash_map.entry(*hash_val).or_default();
            intervals.push(SequilaInterval::new(start.value(i), end.value(i), position))
        });

    Ok(())
}

#[allow(dead_code)]
struct IntervalJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// equijoin columns from the left (build side)
    on_left: Vec<PhysicalExprRef>,
    /// equijoin columns from the right (probe side)
    on_right: Vec<PhysicalExprRef>,
    /// optional join filter
    filter: Option<JoinFilter>,
    /// type of the join (left, right, semi, etc)
    join_type: JoinType,

    left_fut: OnceFut<JoinLeftData>,
    /// right (probe) input
    right: SendableRecordBatchStream,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// Metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
    /// Memory reservation
    reservation: MemoryReservation,

    right_interval: ColInterval,

    state: IntervalJoinStreamState,
    build_side: Option<Arc<JoinLeftData>>,
    hashes_buffer: Vec<u64>,
}

struct ProcessProbeBatchState {
    /// Current probe-side batch
    batch: RecordBatch,
}

enum IntervalJoinStreamState {
    /// Initial state for IntervalJoinStream indicating that build-side data not collected yet
    WaitBuildSide,
    /// Indicates that build-side has been collected, and stream is ready for fetching probe-side
    FetchProbeBatch,
    /// Indicates that non-empty batch has been fetched from probe-side, and is ready to be processed
    ProcessProbeBatch(ProcessProbeBatchState),
    /// Indicates that probe-side has been fully processed
    ExhaustedProbeSide,
}

impl IntervalJoinStreamState {
    /// Tries to extract ProcessProbeBatchState from IntervalJoinStreamState enum.
    /// Returns an error if state is not ProcessProbeBatchState.
    fn try_as_process_probe_batch_mut(&mut self) -> Result<&mut ProcessProbeBatchState> {
        match self {
            IntervalJoinStreamState::ProcessProbeBatch(state) => Ok(state),
            _ => internal_err!("Expected interval join stream in ProcessProbeBatch state"),
        }
    }
}

impl IntervalJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                IntervalJoinStreamState::WaitBuildSide => {
                    handle_state!(ready!(self.collect_build_side(cx)))
                }
                IntervalJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                IntervalJoinStreamState::ProcessProbeBatch(_) => {
                    handle_state!(self.process_probe_batch())
                }
                IntervalJoinStreamState::ExhaustedProbeSide => {
                    log::info!("{:?} finished execution, total processed batches: {:?}, total join time: {:?} ms",
                        std::thread::current().id(),
                        self.join_metrics.output_batches.value(),
                        self.join_metrics.join_time.value()/usize::pow(10, 6)
                    );

                    Poll::Ready(None)
                }
            };
        }
    }

    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        // build hash table from left (build) side, if not yet done
        let left_data = ready!(self.left_fut.get_shared(cx))?;
        build_timer.done();

        log::info!(
            "{:?} is done building a hash table from {:?} batches, {:?} rows, took {:?} ms",
            std::thread::current().id(),
            self.join_metrics.build_input_batches.value(),
            self.join_metrics.build_input_rows.value(),
            self.join_metrics.build_time.value() / usize::pow(10, 6)
        );

        self.state = IntervalJoinStreamState::FetchProbeBatch;
        self.build_side = Some(left_data);

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.right.poll_next_unpin(cx)) {
            None => {
                self.state = IntervalJoinStreamState::ExhaustedProbeSide;
            }
            Some(Ok(batch)) => {
                // Precalculate hash values for fetched batch
                let rights = self
                    .on_right
                    .iter()
                    .map(|c| c.evaluate(&batch)?.into_array(batch.num_rows()))
                    .collect::<Result<Vec<_>>>()?;

                self.hashes_buffer.clear();
                self.hashes_buffer.resize(batch.num_rows(), 0);

                create_hashes(&rights, &self.random_state, &mut self.hashes_buffer)?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                log::debug!(
                    "{:?} is done reading batch {:?}",
                    std::thread::current().id(),
                    self.join_metrics.input_batches.value()
                );

                self.state =
                    IntervalJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState { batch });
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn process_probe_batch(&mut self) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let state = self.state.try_as_process_probe_batch_mut()?;
        let build_side = match self.build_side.as_ref() {
            Some(build_side) => Ok(build_side),
            None => internal_err!("Expected build side in ready state"),
        }?;

        let timer = self.join_metrics.join_time.timer();

        let start = evaluate_as_i32(self.right_interval.start(), &state.batch)?;
        let end = evaluate_as_i32(self.right_interval.end(), &state.batch)?;

        let mut builder_left = PrimitiveBuilder::<UInt32Type>::new();

        let mut rle_right: Vec<u32> = Vec::with_capacity(self.hashes_buffer.len());
        let mut pos_vect: Vec<u32> = Vec::with_capacity(100);
        for (i, hash_val) in self.hashes_buffer.iter().enumerate() {
            build_side
                .hash_map
                .get(*hash_val, start.value(i), end.value(i), |pos| {
                    pos_vect.push(pos as u32);
                });
            match &build_side.hash_map {
                IntervalJoinAlgorithm::CoitresNearest(_t) => {
                    // even if there is no hit we need to preserve the right side
                    rle_right.push(1);
                    if pos_vect.len() == 0 {
                        builder_left.append_null();
                    } else {
                        builder_left.append_slice(&pos_vect);
                    }
                }
                _ => {
                    rle_right.push(pos_vect.len() as u32);
                    builder_left.append_slice(&pos_vect);
                }
            }

            // builder_left.append_slice(&pos_vect);
            pos_vect.clear();
        }
        let left_indexes = builder_left.finish();
        let mut index_right = Vec::with_capacity(left_indexes.len());
        for i in 0..rle_right.len() {
            for _ in 0..rle_right[i] {
                index_right.push(i as u32);
            }
        }
        let right_indexes = PrimitiveArray::from(index_right);

        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(self.schema.fields().len());

        for column_index in &self.column_indices {
            let array: Arc<dyn Array> = if column_index.side == JoinSide::Left {
                let array = build_side.batch.column(column_index.index);
                compute::take(array, &left_indexes, None)?
            } else if column_index.side == JoinSide::Right {
                let array = state.batch.column(column_index.index);
                compute::take(array, &right_indexes, None)?
            } else {
                panic!("Unsupported join_side {:?}", column_index.side);
            };
            columns.push(array);
        }

        let result = RecordBatch::try_new(self.schema.clone(), columns)?;

        self.join_metrics.output_batches.add(1);
        self.join_metrics.output_rows.add(result.num_rows());
        timer.done();

        log::debug!(
            "{:?} is done processing batch {:?} with {:?} output rows",
            std::thread::current().id(),
            self.join_metrics.output_batches.value(),
            result.num_rows()
        );

        self.state = IntervalJoinStreamState::FetchProbeBatch;

        Ok(StatefulStreamResult::Ready(Some(result)))
    }
}

impl RecordBatchStream for IntervalJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for IntervalJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

fn evaluate_as_i32(
    expr: Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<PrimitiveArray<datafusion::arrow::datatypes::Int32Type>> {
    let array = CastExpr::new(expr, DataType::Int32, None)
        .evaluate(batch)?
        .into_array(batch.num_rows())?
        .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
        .to_owned();

    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_context::{Algorithm, SeQuiLaSessionExt, SequilaConfig};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::assert_batches_sorted_eq;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::SortExpr;
    use datafusion::prelude::{col, CsvReadOptions, Expr, SessionConfig, SessionContext};
    use datafusion::test_util::plan_and_collect;

    const READS_PATH: &str = "../../testing/data/interval/reads.csv";
    const TARGETS_PATH: &str = "../../testing/data/interval/targets.csv";

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("contig", DataType::Utf8, false),
            Field::new("pos_start", DataType::Int32, false),
            Field::new("pos_end", DataType::Int64, false),
        ])
    }

    fn csv_options(schema: &Schema) -> CsvReadOptions {
        CsvReadOptions::new()
            .has_header(true)
            .schema(schema)
            .file_extension("csv")
    }

    fn reads_renames() -> Vec<Expr> {
        vec![
            col("contig").alias("reads_contig"),
            col("pos_start").alias("reads_pos_start"),
            col("pos_end").alias("reads_pos_end"),
        ]
    }

    fn target_renames() -> Vec<Expr> {
        vec![
            col("contig").alias("target_contig"),
            col("pos_start").alias("target_pos_start"),
            col("pos_end").alias("target_pos_end"),
        ]
    }

    fn sort_by() -> Vec<SortExpr> {
        vec![
            SortExpr::new(col("reads_contig"), true, true),
            SortExpr::new(col("reads_pos_start"), true, true),
            SortExpr::new(col("reads_pos_end"), true, true),
            SortExpr::new(col("target_contig"), true, true),
            SortExpr::new(col("target_pos_start"), true, true),
            SortExpr::new(col("target_pos_end"), true, true),
        ]
    }

    fn create_context(algorithm: Option<Algorithm>) -> SessionContext {
        let options = ConfigOptions::new();

        let sequila_config = SequilaConfig {
            prefer_interval_join: algorithm.is_some(),
            interval_join_algorithm: algorithm.unwrap_or_default(),
        };

        let config = SessionConfig::from(options)
            .with_option_extension(sequila_config)
            .with_information_schema(true)
            .with_batch_size(2000)
            .with_target_partitions(1);

        SessionContext::new_with_sequila(config)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_all_interval_join_algorithms() -> Result<()> {
        let schema = &schema();

        let algs = [
            None,
            Some(Algorithm::Coitrees),
            Some(Algorithm::IntervalTree),
            Some(Algorithm::ArrayIntervalTree),
            Some(Algorithm::AIList),
        ];

        for alg in algs {
            let ctx = create_context(alg);

            let reads = ctx.read_csv(READS_PATH, csv_options(schema)).await?;
            let targets = ctx.read_csv(TARGETS_PATH, csv_options(schema)).await?;

            let on_expr = [
                col("reads_contig").eq(col("target_contig")),
                col("reads_pos_start").lt_eq(col("target_pos_end")),
                col("reads_pos_end").gt_eq(col("target_pos_start")),
            ];

            let res = reads
                .select(reads_renames())?
                .join_on(targets.select(target_renames())?, JoinType::Inner, on_expr)?
                .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))?
                .sort(sort_by())?;

            if alg.is_none() {
                let expr = format!("{:?}", res.clone().explain(false, false)?.collect().await?);
                assert!(expr.contains("HashJoinExec"))
            }
            res.clone().explain(false, false)?.show().await?;
            res.clone().show().await?;

            let expected = [
                "+--------------+-----------------+---------------+---------------+------------------+----------------+",
                "| reads_contig | reads_pos_start | reads_pos_end | target_contig | target_pos_start | target_pos_end |",
                "+--------------+-----------------+---------------+---------------+------------------+----------------+",
                "| chr1         | 150             | 250           | chr1          | 100              | 190            |",
                "| chr1         | 150             | 250           | chr1          | 200              | 290            |",
                "| chr1         | 190             | 300           | chr1          | 100              | 190            |",
                "| chr1         | 190             | 300           | chr1          | 200              | 290            |",
                "| chr1         | 300             | 501           | chr1          | 400              | 600            |",
                "| chr1         | 500             | 700           | chr1          | 400              | 600            |",
                "| chr1         | 15000           | 15000         | chr1          | 10000            | 20000          |",
                "| chr1         | 22000           | 22300         | chr1          | 22100            | 22100          |",
                "| chr2         | 150             | 250           | chr2          | 100              | 190            |",
                "| chr2         | 150             | 250           | chr2          | 200              | 290            |",
                "| chr2         | 190             | 300           | chr2          | 100              | 190            |",
                "| chr2         | 190             | 300           | chr2          | 200              | 290            |",
                "| chr2         | 300             | 500           | chr2          | 400              | 600            |",
                "| chr2         | 500             | 700           | chr2          | 400              | 600            |",
                "| chr2         | 15000           | 15000         | chr2          | 10000            | 20000          |",
                "| chr2         | 22000           | 22300         | chr2          | 22100            | 22100          |",
                "+--------------+-----------------+---------------+---------------+------------------+----------------+",
            ];

            assert_batches_sorted_eq!(expected, &res.clone().collect().await?);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_projection() -> Result<()> {
        let ctx = create_context(Some(Algorithm::Coitrees));
        ctx.sql("CREATE TABLE a (contig TEXT, start INT, end INT) AS VALUES ('a', 1, 2)")
            .await?;
        ctx.sql("CREATE TABLE b (contig TEXT, start INT, end INT) AS VALUES ('a', 1, 2)")
            .await?;

        let q0 = r#"
            SELECT * FROM a, b
            WHERE a.contig = b.contig AND a.start <= b.end AND a.end >= b.start"#;

        let q1 = r#"
            SELECT a.* FROM a, b
            WHERE a.contig = b.contig AND a.start <= b.end AND a.end >= b.start"#;

        let q2 = r#"
            SELECT b.* FROM a, b
            WHERE a.contig = b.contig AND a.start <= b.end AND a.end >= b.start"#;

        let q3 = r#"
            SELECT b.start, a.end, b.end FROM a, b
            WHERE a.contig = b.contig AND a.start <= b.end AND a.end >= b.start"#;

        for q in [q0, q1, q2, q3] {
            ctx.sql(q).await?;
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_all_interval_join_algorithms_without_equi_condition() -> Result<()> {
        let algs = [
            None,
            Some(Algorithm::Coitrees),
            Some(Algorithm::IntervalTree),
            Some(Algorithm::ArrayIntervalTree),
            Some(Algorithm::AIList),
            Some(Algorithm::Lapper),
        ];

        let schema = &schema();

        for alg in algs {
            let ctx = create_context(alg);

            let reads = ctx.read_csv(READS_PATH, csv_options(schema)).await?;
            let targets = ctx.read_csv(TARGETS_PATH, csv_options(schema)).await?;

            let on_expr = [
                col("reads_pos_start").lt_eq(col("target_pos_end")),
                col("reads_pos_end").gt_eq(col("target_pos_start")),
            ];

            let res = reads
                .select(reads_renames())?
                .join_on(targets.select(target_renames())?, JoinType::Inner, on_expr)?
                .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))?
                .sort(sort_by())?;

            if alg.is_none() {
                let expr = format!("{:?}", res.clone().explain(false, false)?.collect().await?);
                assert!(expr.contains("NestedLoopJoinExec"))
            }

            res.clone().explain(false, false)?.show().await?;
            res.clone().show().await?;

            let expected = [
                "+--------------+-----------------+---------------+---------------+------------------+----------------+",
                "| reads_contig | reads_pos_start | reads_pos_end | target_contig | target_pos_start | target_pos_end |",
                "+--------------+-----------------+---------------+---------------+------------------+----------------+",
                "| chr1         | 150             | 250           | chr1          | 100              | 190            |",
                "| chr1         | 150             | 250           | chr1          | 200              | 290            |",
                "| chr1         | 150             | 250           | chr2          | 100              | 190            |",
                "| chr1         | 150             | 250           | chr2          | 200              | 290            |",
                "| chr1         | 190             | 300           | chr1          | 100              | 190            |",
                "| chr1         | 190             | 300           | chr1          | 200              | 290            |",
                "| chr1         | 190             | 300           | chr2          | 100              | 190            |",
                "| chr1         | 190             | 300           | chr2          | 200              | 290            |",
                "| chr1         | 300             | 501           | chr1          | 400              | 600            |",
                "| chr1         | 300             | 501           | chr2          | 400              | 600            |",
                "| chr1         | 500             | 700           | chr1          | 400              | 600            |",
                "| chr1         | 500             | 700           | chr2          | 400              | 600            |",
                "| chr1         | 15000           | 15000         | chr1          | 10000            | 20000          |",
                "| chr1         | 15000           | 15000         | chr2          | 10000            | 20000          |",
                "| chr1         | 22000           | 22300         | chr1          | 22100            | 22100          |",
                "| chr1         | 22000           | 22300         | chr2          | 22100            | 22100          |",
                "| chr2         | 150             | 250           | chr1          | 100              | 190            |",
                "| chr2         | 150             | 250           | chr1          | 200              | 290            |",
                "| chr2         | 150             | 250           | chr2          | 100              | 190            |",
                "| chr2         | 150             | 250           | chr2          | 200              | 290            |",
                "| chr2         | 190             | 300           | chr1          | 100              | 190            |",
                "| chr2         | 190             | 300           | chr1          | 200              | 290            |",
                "| chr2         | 190             | 300           | chr2          | 100              | 190            |",
                "| chr2         | 190             | 300           | chr2          | 200              | 290            |",
                "| chr2         | 300             | 500           | chr1          | 400              | 600            |",
                "| chr2         | 300             | 500           | chr2          | 400              | 600            |",
                "| chr2         | 500             | 700           | chr1          | 400              | 600            |",
                "| chr2         | 500             | 700           | chr2          | 400              | 600            |",
                "| chr2         | 15000           | 15000         | chr1          | 10000            | 20000          |",
                "| chr2         | 15000           | 15000         | chr2          | 10000            | 20000          |",
                "| chr2         | 22000           | 22300         | chr1          | 22100            | 22100          |",
                "| chr2         | 22000           | 22300         | chr2          | 22100            | 22100          |",
                "+--------------+-----------------+---------------+---------------+------------------+----------------+",
            ];

            assert_batches_sorted_eq!(expected, &res.clone().collect().await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_datatype() -> Result<()> {
        let ctx = create_context(Some(Algorithm::default()));
        let max_plus_one = i64::from(i32::MAX) + 1;

        let create_table_a_query = r#"
            CREATE TABLE a (contig VARCHAR NOT NULL, start BIGINT NOT NULL, end BIGINT NOT NULL)
            AS VALUES ('a', 1, 2), ('b', 1, 4)
        "#;

        let create_table_b_query = format!(
            r#"
            CREATE TABLE b (contig VARCHAR NOT NULL, start BIGINT NOT NULL, end BIGINT NOT NULL)
            AS VALUES ('a', 1, 2), ('b', 3, {})
        "#,
            max_plus_one
        );

        ctx.sql(create_table_a_query).await?;
        ctx.sql(create_table_b_query.as_str()).await?;

        let select_query = r#"
            SELECT * FROM a
            JOIN b ON
                a.contig = b.contig AND
                a.end >= b.start AND
                a.start <= b.end
        "#;

        let err: Result<_> = plan_and_collect(&ctx, select_query).await;

        assert!(err.is_err());
        assert_eq!(
            format!(
                "Arrow error: Cast error: Can't cast value {} to type Int32",
                max_plus_one
            ),
            err.unwrap_err().to_string(),
        );

        Ok(())
    }
}
