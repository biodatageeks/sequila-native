use crate::physical_planner::joins::utils::{
    estimate_join_statistics, BuildProbeJoinMetrics, OnceAsync, OnceFut,
};
use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray, RecordBatch, UInt32Array, UInt32BufferBuilder};
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::{Schema, SchemaRef};
use coitrees::*;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{project_schema, Result};
use datafusion::common::{internal_err, plan_err, DataFusionError, JoinSide, JoinType, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Operator, UserDefinedLogicalNode};
use datafusion::physical_expr::equivalence::{join_equivalence_properties, ProjectionMapping};
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, UnKnownColumn};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalExprRef, PhysicalSortExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionMode, ExecutionPlanProperties};
use datafusion::physical_plan::joins::utils::{adjust_right_output_partitioning, JoinOnRef};
use datafusion::physical_plan::joins::utils::calculate_join_output_ordering;
use datafusion::physical_plan::joins::utils::check_join_is_valid;
use datafusion::physical_plan::joins::utils::partitioned_join_output_partitioning;
use datafusion::physical_plan::joins::utils::{build_join_schema, JoinOn};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use fnv::FnvHashMap;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use log::info;
use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;
use std::task::Poll;
use std::{fmt, thread};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_plan::common::can_project;

type Metadata = usize;

pub struct IntervalJoinHashMap {
    // Stores hash value to last row index
    map: FnvHashMap<u64, coitrees::COITree<Metadata, u32>>,
}

impl Debug for IntervalJoinHashMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let x = self
            .map
            .iter()
            .map(|(key, val)| (key, val.iter().collect::<Vec<_>>()));

        f.debug_map().entries(x.into_iter()).finish()
    }
}

impl IntervalJoinHashMap {
    pub fn new(map: FnvHashMap<u64, COITree<Metadata, u32>>) -> Self {
        Self { map }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            map: FnvHashMap::with_capacity_and_hasher(capacity, Default::default()),
        }
    }
}

#[derive(Debug)]
struct JoinLeftData {
    hash_map: IntervalJoinHashMap,
    batch: RecordBatch,
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl JoinLeftData {
    fn new(
        hash_map: IntervalJoinHashMap,
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
}

impl IntervalJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        projection: Option<Vec<usize>>,
        partition_mode: PartitionMode,
        null_equals_null: bool,
    ) -> datafusion::common::Result<Self> {
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
            join_type: *join_type,
            join_schema: join_schema,
            left_fut: Default::default(),
            random_state,
            mode: partition_mode,
            metrics: ExecutionPlanMetricsSet::new(),
            projection,
            column_indices,
            null_equals_null,
            cache
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
            &self.join_type,
            projection,
            self.mode,
            self.null_equals_null,
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
                JoinType::Inner | JoinType::Right => adjust_right_output_partitioning(
                    right.output_partitioning(),
                    left_columns_len,
                ),
                JoinType::RightSemi | JoinType::RightAnti => {
                    right.output_partitioning().clone()
                }
                JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::Full => Partitioning::UnknownPartitioning(
                    right.output_partitioning().partition_count(),
                ),
            },
            PartitionMode::Partitioned => partitioned_join_output_partitioning(
                join_type,
                left.output_partitioning(),
                right.output_partitioning(),
                left_columns_len,
            ),
            PartitionMode::Auto => Partitioning::UnknownPartitioning(
                right.output_partitioning().partition_count(),
            ),
        };

        // Determine execution mode by checking whether this join is pipeline
        // breaking. This happens when the left side is unbounded, or the right
        // side is unbounded with `Left`, `Full`, `LeftAnti` or `LeftSemi` join types.
        let pipeline_breaking = left.execution_mode().is_unbounded()
            || (right.execution_mode().is_unbounded()
            && matches!(
                    join_type,
                    JoinType::Left
                        | JoinType::Full
                        | JoinType::LeftAnti
                        | JoinType::LeftSemi
                ));

        let mode = if pipeline_breaking {
            ExecutionMode::PipelineBreaking
        } else {
            crate::physical_planner::joins::utils::execution_mode_from_children([left, right])
        };

        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            let projection_exprs = project_index_to_exprs(projection, &schema);
            // construct a map from the input expressions to the output expression of the Projection
            let projection_mapping =
                ProjectionMapping::try_new(&projection_exprs, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
            if let Partitioning::Hash(exprs, part) = output_partitioning {
                let normalized_exprs = exprs
                    .iter()
                    .map(|expr| {
                        eq_properties
                            .project_expr(expr, &projection_mapping)
                            .unwrap_or_else(|| {
                                Arc::new(UnKnownColumn::new(&expr.to_string()))
                            })
                    })
                    .collect();
                output_partitioning = Partitioning::Hash(normalized_exprs, part);
            }
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            mode,
        ))
    }
}

fn project_index_to_exprs(
    projection_index: &[usize],
    schema: &SchemaRef,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    projection_index
        .iter()
        .map(|index| {
            let field = schema.field(*index);
            (
                Arc::new(Column::new(
                    field.name(),
                    *index,
                )) as Arc<dyn PhysicalExpr>,
                field.name().to_owned(),
            )
        })
        .collect::<Vec<_>>()
}

impl DisplayAs for IntervalJoinExec {
    //TODO: Update for the IntervalSearchJoinExec
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_filter = self.filter.as_ref().map_or_else(
                    || "".to_string(),
                    |f| format!(", filter={}", f.expression()),
                );
                let on = self
                    .on
                    .iter()
                    .map(|(c1, c2)| format!("({}, {})", c1, c2))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(
                    f,
                    "IntervalJoinExec: mode={:?}, join_type={:?}, on=[{}]{}",
                    self.mode, self.join_type, on, display_filter
                )
            }
        }
    }
}

impl ExecutionPlan for IntervalJoinExec {
    fn name(&self) -> &'static str {
        "HashJoinExec"
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
            &self.join_type,
            self.projection.clone(),
            self.mode,
            self.null_equals_null,
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

        let (left_interval, right_interval) = self.filter().and_then(parse_filter)
            .expect(format!("couldn't parse {:?}", self.filter()).as_str());

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
                    left_interval
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

        Ok(Box::pin(IntervalJoinStream {
            schema: self.schema(),
            on_left,
            on_right,
            filter: self.filter.clone(),
            join_type: self.join_type,
            left_fut,
            right: right_stream,
            column_indices: self.column_indices.clone(),
            random_state: self.random_state.clone(),
            join_metrics,
            null_equals_null: self.null_equals_null,
            reservation,
            right_interval,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        let mut stats = estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            &self.join_type,
            &self.join_schema,
        )?;
        // Project statistics if there is a projection
        if let Some(projection) = &self.projection {
            stats.column_statistics = stats
                .column_statistics
                .into_iter()
                .enumerate()
                .filter(|(i, _)| projection.contains(i))
                .map(|(_, s)| s)
                .collect();
        }
        Ok(stats)
    }
}

async fn collect_left_input(
    partition: Option<usize>,
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<PhysicalExprRef>,
    context: Arc<TaskContext>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    left_interval: ColInterval
) -> datafusion::common::Result<JoinLeftData> {
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
        16 * estimated_buckets + estimated_buckets + size_of::<IntervalJoinHashMap>();

    reservation.try_grow(estimated_hastable_size)?;
    metrics.build_mem_used.add(estimated_hastable_size);

    let mut hashmap = std::collections::HashMap::<u64, Vec<Interval<Metadata>>>::new();
    let mut hashes_buffer = Vec::new();
    let mut offset = 0;

    // Updating hashmap starting from the last batch
    for batch in &batches {
        // build a left hash map
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hashmap(
            &on_left,
            &left_interval,
            &batch,
            &mut hashmap,
            offset,
            &random_state,
            &mut hashes_buffer,
        )?;
        offset += batch.num_rows();
    }

    let hashmap = hashmap
        .iter()
        .map(|(k, v)| (*k, COITree::new(v)))
        .collect::<FnvHashMap<u64, COITree<Metadata, u32>>>();

    let hashmap = IntervalJoinHashMap::new(hashmap);
    let single_batch = concat_batches(&schema, &batches)?;
    let data = JoinLeftData::new(hashmap, single_batch, reservation);

    Ok(data)
}

fn update_hashmap(
    on: &[PhysicalExprRef],
    left_interval: &ColInterval,
    batch: &RecordBatch,
    hash_map: &mut std::collections::HashMap<u64, Vec<Interval<Metadata>>>,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
) -> Result<()> {
    let keys_values = on
        .iter()
        .map(|c| c.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<Result<Vec<_>>>()?;

    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;

    let hash_values_iter = hash_values
        .iter()
        .enumerate()
        .map(|(i, val)| (i + offset, val))
        .collect::<Vec<_>>();

    let start = left_interval.start.evaluate(batch)?.into_array(batch.num_rows())?;
    let start = start.as_primitive::<arrow::datatypes::Int64Type>();

    let end = left_interval.end.evaluate(batch)?.into_array(batch.num_rows())?;
    let end = end.as_primitive::<arrow::datatypes::Int64Type>();

    for i in 0..batch.num_rows() {
        let (position, hash_val) = hash_values_iter[i];
        let intervals = hash_map.entry(*hash_val).or_insert(Vec::new());
        let start = start.value(i) as i32;
        let end = end.value(i) as i32;
        intervals.push(Interval::new(start, end, position))
    }

    Ok(())
}

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
}

impl IntervalJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<datafusion::common::Result<RecordBatch>>> {
        let left_data: &JoinLeftData = match ready!(self.left_fut.get(cx)) {
            Ok(left_data) => left_data,
            Err(e) => return Poll::Ready(Some(Err(e))),
        };

        self.right
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let rights = self
                        .on_right
                        .iter()
                        .map(|c| c.evaluate(&batch)?.into_array(batch.num_rows()))
                        .collect::<Result<Vec<_>>>()
                        .ok()?;

                    let mut hashes_buffer: Vec<u64> = Vec::new();
                    hashes_buffer.clear();
                    hashes_buffer.resize(batch.num_rows(), 0);
                    create_hashes(&rights, &self.random_state, &mut hashes_buffer).ok()?;

                    let start = self.right_interval.start.evaluate(&batch).ok()?.into_array(batch.num_rows()).ok()?;
                    let start = start.as_primitive::<arrow::datatypes::Int64Type>();

                    let end = self.right_interval.end.evaluate(&batch).ok()?.into_array(batch.num_rows()).ok()?;
                    let end = end.as_primitive::<arrow::datatypes::Int64Type>();

                    let mut left_builder = UInt32BufferBuilder::new(0);
                    let mut right_builder = UInt32BufferBuilder::new(0);

                    for (i, hash_val) in hashes_buffer.into_iter().enumerate() {
                        left_data.hash_map.map.get(&hash_val).map(|tree| {
                            let start = start.value(i) as i32;
                            let end = end.value(i) as i32;
                            tree.query(start, end, |node| {
                                left_builder.append(node.metadata as u32);
                                right_builder.append(i as u32);
                            });
                        });
                    }

                    let left_indexes: UInt32Array =
                        PrimitiveArray::new(left_builder.finish().into(), None);
                    let right_indexes: UInt32Array =
                        PrimitiveArray::new(right_builder.finish().into(), None);

                    let mut columns: Vec<Arc<dyn Array>> =
                        Vec::with_capacity(self.schema.fields().len());

                    for c in left_data.batch.columns() {
                        columns.push(arrow::compute::take(c, &left_indexes, None).ok()?);
                    }

                    for c in batch.columns() {
                        columns.push(arrow::compute::take(c, &right_indexes, None).ok()?);
                    }

                    Some(Ok(RecordBatch::try_new(self.schema.clone(), columns).ok()?))
                }
                other => other,
            })
    }
}

impl RecordBatchStream for IntervalJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for IntervalJoinStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

trait FromPhysicalExpr {
    fn to_binary(&self) -> Option<&BinaryExpr>;
    fn to_column(&self) -> Option<&Column>;
    fn to_cast_expr(&self) -> Option<&CastExpr>;
}

impl FromPhysicalExpr for dyn PhysicalExpr {
    fn to_binary(&self) -> Option<&BinaryExpr> { self.as_any().downcast_ref::<BinaryExpr>() }
    fn to_column(&self) -> Option<&Column> { self.as_any().downcast_ref::<Column>() }
    fn to_cast_expr(&self) -> Option<&CastExpr> { self.as_any().downcast_ref::<CastExpr>() }
}


#[derive(Debug)]
struct ColInterval {
    start: Arc<dyn PhysicalExpr>,
    end: Arc<dyn PhysicalExpr>
}

impl ColInterval {
    fn new(start: Arc<dyn PhysicalExpr>, end: Arc<dyn PhysicalExpr>) -> Self {
        // assert_eq!(start.side, end.side, "both columns must be from the same side");
        ColInterval { start, end }
    }
}

fn get_with_source_index(node: &Arc<dyn PhysicalExpr>, indices: &[ColumnIndex]) -> Arc<dyn PhysicalExpr> {
    node.clone().transform_up(|node| {
        if let Some(column) = node.to_column() {
            let new_column = Column::new(column.name(), indices[column.index()].index);
            let result = Arc::new(new_column) as Arc<dyn PhysicalExpr>;
            Ok(Transformed::yes(result))
        } else {
            Ok(Transformed::no(node))
        }
    }).data().unwrap()
}

//TODO Add support for datatype, currently expected to be Int64
fn parse_filter(filter: &JoinFilter) -> Option<(ColInterval, ColInterval)> {
    let expr = filter.expression().to_binary()?;
    if matches!(expr.op(), Operator::And) {
        let left = expr.left().to_binary()?;
        let right = expr.right().to_binary()?;

        let indices = filter.column_indices();

        match (left.op(), right.op()) {
            // assume that LEFT_END >= RIGHT_START in LEFT and LEFT_START <= RIGHT_END in RIGHT
            (Operator::GtEq, Operator::LtEq) => {
                Some((
                    ColInterval::new(
                        get_with_source_index(right.left(), indices),
                        get_with_source_index(left.left(), indices),
                    ),
                    ColInterval::new(
                        get_with_source_index(left.right(), indices),
                        get_with_source_index(right.right(), indices),
                    )
                ))
            },
            // assume that LEFT_START <= RIGHT_END in LEFT and LEFT_END >= RIGHT_START in RIGHT
            (Operator::LtEq, Operator::GtEq) => {

                Some((
                    ColInterval::new(
                        get_with_source_index(left.left(), indices),
                        get_with_source_index(right.left(), indices),
                    ),
                    ColInterval::new(
                        get_with_source_index(right.right(), indices),
                        get_with_source_index(left.right(), indices),
                    )
                ))
            },
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinSide;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{ExprSchemable, Operator};
    use datafusion::physical_plan::expressions::{BinaryExpr, Column};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::prelude::*;
    use std::sync::Arc;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::physical_expr::expressions::CastExpr;
    use crate::physical_planner::interval_join::parse_filter;

    use crate::session_context::SeQuiLaSessionExt;

    const READS_PATH: &str = "../../testing/data/interval/reads.csv";
    const TARGETS_PATH: &str = "../../testing/data/interval/targets.csv";

    #[tokio::test]
    async fn init() -> datafusion::common::Result<()> {
        let schema = Schema::new(vec![
            Field::new("contig", DataType::Utf8, false),
            Field::new("pos_start", DataType::Int32, false),
            Field::new("pos_end", DataType::Int64, false),
        ]);

        let options = ConfigOptions::new();

        let config = SessionConfig::from(options)
            .with_batch_size(2000)
            .with_target_partitions(1);

        let ctx = SessionContext::new_with_sequila(config);
        // let ctx = SessionContext::new();
        let csv_options = CsvReadOptions::new()
            .has_header(true)
            .schema(&schema)
            .file_extension("csv");
        let reads: DataFrame = ctx.read_csv(READS_PATH, csv_options.clone()).await?;
        let targets = ctx.read_csv(TARGETS_PATH, csv_options).await?;

        let reads_renames = vec![
            col("contig").alias("reads_contig"),
            col("pos_start").alias("reads_pos_start"),
            col("pos_end").alias("reads_pos_end"),
            // cast(col("pos_start").alias("reads_pos_start"), DataType::Int64),
        ];
        let target_renames = vec![
            col("contig").alias("target_contig"),
            col("pos_start").alias("target_pos_start"),
            col("pos_end").alias("target_pos_end"),
        ];

        let on_expr = [
                col("reads_contig").eq(col("target_contig")),
                col("reads_pos_start").lt_eq(col("target_pos_end")),
                // col("c2").gt(col("b1")),
                col("reads_pos_end").gt_eq(col("target_pos_start")), // col("b2").lt(col("c1"))
            ];

        let res = reads
            .select(reads_renames)?
            .join_on(
                targets.select(target_renames)?,
                JoinType::Inner,
                // a.column2 >= b.column1 AND a.column1 <= b.column2
                on_expr,
            )?
            .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))?;

        let x = res.clone().create_physical_plan().await?;
        // println!("execution_plan = {:?}", x);

        res.clone().explain(false, false)?.show().await?;

        // println!("\n------------------------\n");
        // println!("b={}", res.clone().collect().await?.len());
        // println!("---");
        res.clone().show().await?;

        // select * from targets
        // join reads on
        // targets.contig = reads.contig
        // and targets.pos_start >= reads.pos_start
        // and targets.pos_end <= reads.pos_end;
        let expected = [
            "+--------------+-----------------+---------------+---------------+------------------+----------------+",
            "| reads_contig | reads_pos_start | reads_pos_end | target_contig | target_pos_start | target_pos_end |",
            "+--------------+-----------------+---------------+---------------+------------------+----------------+",
            "| chr1         | 150             | 250           | chr1          | 100              | 190            |",
            "| chr1         | 190             | 300           | chr1          | 100              | 190            |",
            "| chr1         | 150             | 250           | chr1          | 200              | 290            |",
            "| chr1         | 190             | 300           | chr1          | 200              | 290            |",
            "| chr1         | 300             | 501           | chr1          | 400              | 600            |",
            "| chr1         | 500             | 700           | chr1          | 400              | 600            |",
            "| chr1         | 15000           | 15000         | chr1          | 10000            | 20000          |",
            "| chr1         | 22000           | 22300         | chr1          | 22100            | 22100          |",
            "| chr2         | 150             | 250           | chr2          | 100              | 190            |",
            "| chr2         | 190             | 300           | chr2          | 100              | 190            |",
            "| chr2         | 150             | 250           | chr2          | 200              | 290            |",
            "| chr2         | 190             | 300           | chr2          | 200              | 290            |",
            "| chr2         | 300             | 500           | chr2          | 400              | 600            |",
            "| chr2         | 500             | 700           | chr2          | 400              | 600            |",
            "| chr2         | 15000           | 15000         | chr2          | 10000            | 20000          |",
            "| chr2         | 22000           | 22300         | chr2          | 22100            | 22100          |",
            "+--------------+-----------------+---------------+---------------+------------------+----------------+",
        ];

        assert_batches_sorted_eq!(expected, &res.clone().collect().await?);

        Ok(())
    }

    #[test]
    fn parse_filter_to_start_end() {
        let lstart = "lstart";
        let lend = "lend";
        let rstart = "rstart";
        let rend = "rend";

        let gteq = BinaryExpr::new(
            Arc::new(Column::new(lend, 1)),
            Operator::GtEq,
            Arc::new(Column::new(rstart, 2)),
        );
        let lteq = BinaryExpr::new(
            Arc::new(Column::new(lstart, 0)),
            Operator::LtEq,
            Arc::new(CastExpr::new(Arc::new(Column::new(rend, 3)), DataType::Int64, None)),
        );
        let expression = BinaryExpr::new(Arc::new(gteq), Operator::And, Arc::new(lteq));

        let column_indices = JoinFilter::build_column_indices(vec![1, 2], vec![1, 2]);

        let schema = Schema::new(vec![
            Field::new(lstart, DataType::Int64, false),
            Field::new(lend, DataType::Int64, false),
            Field::new(rstart, DataType::Int64, false),
            Field::new(rend, DataType::Int64, false),
        ]);

        // who builds it?
        let filter = JoinFilter::new(Arc::new(expression), column_indices, schema);
        println!("{:#?}", filter);

        dbg!(parse_filter(&filter));

        let fields = filter.schema().fields().into_iter();
        let zipped = filter
            .column_indices()
            .iter()
            .zip(fields)
            .collect::<Vec<_>>();

        fn to_binary_expr(expr: &Arc<dyn PhysicalExpr>) -> Option<&BinaryExpr> {
            expr.as_any().downcast_ref::<BinaryExpr>()
        }

        fn parse_leaf(
            expr: &BinaryExpr,
        ) -> Option<(&Column, &Column, &datafusion::logical_expr::Operator)> {
            let l = expr.left().as_any().downcast_ref::<Column>();
            let r = expr.right().as_any().downcast_ref::<Column>();
            l.zip(r).map(|(l, r)| (l, r, expr.op()))
        }

        let x = to_binary_expr(filter.expression()).and_then(|expr| {
            let left = to_binary_expr(expr.left()).and_then(parse_leaf);
            let right = to_binary_expr(expr.right()).and_then(parse_leaf);
            left.zip(right).map(|((ll, lr, lop), (rl, rr, rop))| {
                let cols = vec![ll, lr, rl, rr];
                if (*lop == Operator::GtEq && *rop == Operator::LtEq) {
                    // assume that LEFT_END >= RIGHT_START in LEFT and LEFT_START <= RIGHT_END in RIGHT
                    let left_start = zipped[rl.index()];
                    let left_end = zipped[ll.index()];

                    let right_start = zipped[lr.index()];
                    let right_end = zipped[rr.index()];

                    ((left_start, left_end), (right_start, right_end))
                } else if (*lop == Operator::LtEq && *rop == Operator::GtEq) {
                    // assume that LEFT_START <= RIGHT_END in LEFT and LEFT_END >= RIGHT_START in RIGHT

                    let left_start = zipped[ll.index()];
                    let left_end = zipped[rl.index()];

                    let right_start = zipped[rr.index()];
                    let right_end = zipped[lr.index()];

                    ((left_start, left_end), (right_start, right_end))
                } else {
                    panic!("cannot parse")
                }
            })
        });

        // println!("{:#?}", x);

        /*
        JoinFilter {
            expression: BinaryExpr {
                left: BinaryExpr {
                    left: Column {
                        name: "lend",
                        index: 1,
                    },
                    op: GtEq,
                    right: Column {
                        name: "rstart",
                        index: 2,
                    },
                },
                op: And,
                right: BinaryExpr {
                    left: Column {
                        name: "lstart",
                        index: 0,
                    },
                    op: LtEq,
                    right: Column {
                        name: "rend",
                        index: 3,
                    },
                },
            },
            column_indices: [
                ColumnIndex {
                    index: 1,
                    side: Left,
                },
                ColumnIndex {
                    index: 2,
                    side: Left,
                },
                ColumnIndex {
                    index: 1,
                    side: Right,
                },
                ColumnIndex {
                    index: 2,
                    side: Right,
                },
            ],
            schema: Schema {
                fields: [
                    Field {
                        name: "lstart",
                        data_type: Int64,
                        nullable: false,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "lend",
                        data_type: Int64,
                        nullable: false,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "rstart",
                        data_type: Int64,
                        nullable: false,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "rend",
                        data_type: Int64,
                        nullable: false,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                ],
                metadata: {},
            },
        }
                 */
    }
}
