use crate::physical_planner::joins::utils::{
    estimate_join_statistics, BuildProbeJoinMetrics, JoinHashMapType, OnceAsync, OnceFut,
};
use crate::physical_planner::joins::IntervalSearchJoinExec;
use ahash::RandomState;
use arrow::array::{
    Array, AsArray, PrimitiveArray, RecordBatch, UInt32Array, UInt32BufferBuilder, UInt64Array,
    UInt64BufferBuilder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::{datatypes as adt, downcast_primitive_array};
use coitrees::*;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::Result;
use datafusion::common::{internal_err, plan_err, DataFusionError, JoinSide, JoinType, Statistics};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::equivalence::join_equivalence_properties;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::utils::adjust_right_output_partitioning;
use datafusion::physical_plan::joins::utils::calculate_join_output_ordering;
use datafusion::physical_plan::joins::utils::check_join_is_valid;
use datafusion::physical_plan::joins::utils::partitioned_join_output_partitioning;
use datafusion::physical_plan::joins::utils::{build_join_schema, JoinOn};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use fnv::FnvHashMap;
use futures::{ready, Stream, StreamExt, TryStreamExt};
use log::info;
use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;
use std::task::Poll;
use std::{fmt, thread};

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
    pub on: Vec<(Column, Column)>,
    /// Filters which are applied while finding matching rows
    pub filter: Option<JoinFilter>,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The output schema for the join
    schema: SchemaRef,
    /// Future that consumes left input and builds the hash table
    left_fut: OnceAsync<JoinLeftData>,
    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
    /// Output order
    output_order: Option<Vec<PhysicalSortExpr>>,
    /// Partitioning mode to use
    pub mode: PartitionMode,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Null matching behavior: If `null_equals_null` is true, rows that have
    /// `null`s in both left and right equijoin columns will be matched.
    /// Otherwise, rows that have `null`s in the join columns will not be
    /// matched and thus will not appear in the output.
    pub null_equals_null: bool,
}

impl IntervalJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        filter: Option<JoinFilter>,
        join_type: &JoinType,
        partition_mode: PartitionMode,
        null_equals_null: bool,
    ) -> datafusion::common::Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        if on.is_empty() {
            return plan_err!("On constraints in HashJoinExec should be non-empty");
        }

        check_join_is_valid(&left_schema, &right_schema, &on)?;

        let (schema, column_indices) = build_join_schema(&left_schema, &right_schema, join_type);

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let output_order = calculate_join_output_ordering(
            left.output_ordering().unwrap_or(&[]),
            right.output_ordering().unwrap_or(&[]),
            *join_type,
            &on,
            left_schema.fields.len(),
            &Self::maintains_input_order(*join_type),
            Some(Self::probe_side()),
        );

        Ok(IntervalJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            schema: Arc::new(schema),
            left_fut: Default::default(),
            random_state,
            mode: partition_mode,
            metrics: ExecutionPlanMetricsSet::new(),
            column_indices,
            null_equals_null,
            output_order,
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
    pub fn on(&self) -> &[(Column, Column)] {
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.mode {
            PartitionMode::CollectLeft => vec![
                Distribution::SinglePartition,
                Distribution::UnspecifiedDistribution,
            ],
            PartitionMode::Partitioned => {
                let (left_expr, right_expr) = self
                    .on
                    .iter()
                    .map(|(l, r)| {
                        (
                            Arc::new(l.clone()) as Arc<dyn PhysicalExpr>,
                            Arc::new(r.clone()) as Arc<dyn PhysicalExpr>,
                        )
                    })
                    .unzip();
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

    fn unbounded_output(&self, children: &[bool]) -> datafusion::common::Result<bool> {
        let (left, right) = (children[0], children[1]);
        // If left is unbounded, or right is unbounded with JoinType::Right,
        // JoinType::Full, JoinType::RightAnti types.
        let breaking = left
            || (right
                && matches!(
                    self.join_type,
                    JoinType::Left | JoinType::Full | JoinType::LeftAnti | JoinType::LeftSemi
                ));

        if breaking {
            plan_err!(
                "Join Error: The join with cannot be executed with unbounded inputs. {}",
                if left && right {
                    "Currently, we do not support unbounded inputs on both sides."
                } else {
                    "Please consider a different type of join or sources."
                }
            )
        } else {
            Ok(left || right)
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        let left_columns_len = self.left.schema().fields.len();
        match self.mode {
            PartitionMode::CollectLeft => match self.join_type {
                JoinType::Inner | JoinType::Right => adjust_right_output_partitioning(
                    self.right.output_partitioning(),
                    left_columns_len,
                ),
                JoinType::RightSemi | JoinType::RightAnti => self.right.output_partitioning(),
                JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::Full => {
                    Partitioning::UnknownPartitioning(
                        self.right.output_partitioning().partition_count(),
                    )
                }
            },
            PartitionMode::Partitioned => partitioned_join_output_partitioning(
                self.join_type,
                self.left.output_partitioning(),
                self.right.output_partitioning(),
                left_columns_len,
            ),
            PartitionMode::Auto => Partitioning::UnknownPartitioning(
                self.right.output_partitioning().partition_count(),
            ),
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.output_order.as_deref()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order(self.join_type)
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        join_equivalence_properties(
            self.left.equivalence_properties(),
            self.right.equivalence_properties(),
            &self.join_type,
            self.schema(),
            &self.maintains_input_order(),
            Some(Self::probe_side()),
            self.on(),
        )
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IntervalSearchJoinExec::try_new(
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.mode,
            self.null_equals_null,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        // TODO stats: it is not possible in general to know the output size of joins
        // There are some special cases though, for example:
        // - `A LEFT JOIN B ON A.col=B.col` with `COUNT_DISTINCT(B.col)=COUNT(B.col)`
        estimate_join_statistics(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            &self.join_type,
            &self.schema,
        )
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

        let start_end = self.filter().and_then(find_start_end).unwrap();

        let left_start_end = start_end.0;
        let right_start_end = start_end.1;

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
                    (left_start_end.0 .0.clone(), left_start_end.1 .0.clone()),
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
                    (right_start_end.0 .0.clone(), right_start_end.1 .0.clone()),
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
        }))
    }
}

async fn collect_left_input(
    partition: Option<usize>,
    random_state: RandomState,
    left: Arc<dyn ExecutionPlan>,
    on_left: Vec<Column>,
    context: Arc<TaskContext>,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    start_end: (ColumnIndex, ColumnIndex),
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

    let start_end = vec![start_end.0, start_end.1];

    // Updating hashmap starting from the last batch
    for batch in &batches {
        // build a left hash map
        hashes_buffer.clear();
        hashes_buffer.resize(batch.num_rows(), 0);
        update_hashmap(
            &on_left,
            &start_end,
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
    on: &[Column],
    start_end: &[ColumnIndex],
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

    let start_end = start_end
        .iter()
        .map(|c| batch.column(c.index))
        .collect::<Vec<_>>();

    let start = start_end[0].as_primitive::<arrow::datatypes::Int64Type>();
    let end = start_end[1].as_primitive::<arrow::datatypes::Int64Type>();

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
    on_left: Vec<Column>,
    /// equijoin columns from the right (probe side)
    on_right: Vec<Column>,
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

        let start_end = self.filter.as_ref().and_then(find_start_end).unwrap().1;
        let right_start = start_end.0;
        let right_end = start_end.1;

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

                    let start_end = vec![right_start.0.index, right_end.0.index]
                        .iter()
                        .map(|c| batch.column(*c))
                        .collect::<Vec<_>>();

                    let start = start_end[0].as_primitive::<arrow::datatypes::Int64Type>();
                    let end = start_end[1].as_primitive::<arrow::datatypes::Int64Type>();

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

// Some(
// BinaryExpr {
// left: BinaryExpr {
//      left: Column { name: "b1", index: 0 }, op: GtEq, right: Column { name: "b2", index: 2 } },
// op: And,
// right: BinaryExpr {
//      left: Column { name: "c1", index: 1 }, op: LtEq, right: Column { name: "c2", index: 3 } }
// })

#[derive(Debug)]
struct ColToSelect {
    left: (String, String, Operator),
    right: (String, String, Operator),
}

fn parse_filter(filter: &JoinFilter) -> ColToSelect {
    let be = filter
        .expression()
        .as_any()
        .downcast_ref::<BinaryExpr>()
        .unwrap();
    if (*be.op() == Operator::And) {
        let left = be.left().as_any().downcast_ref::<BinaryExpr>().unwrap();
        let ll = left
            .left()
            .as_any()
            .downcast_ref::<Column>()
            .unwrap()
            .name()
            .to_string();
        let lr = left
            .right()
            .as_any()
            .downcast_ref::<Column>()
            .unwrap()
            .name()
            .to_string();
        let right = be.right().as_any().downcast_ref::<BinaryExpr>().unwrap();
        let rl = right
            .left()
            .as_any()
            .downcast_ref::<Column>()
            .unwrap()
            .name()
            .to_string();
        let rr = right
            .right()
            .as_any()
            .downcast_ref::<Column>()
            .unwrap()
            .name()
            .to_string();
        return ColToSelect {
            left: (ll, lr, *left.op()),
            right: (rl, rr, *right.op()),
        };
    } else {
        panic!("wtf");
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

fn find_start_end(
    filter: &JoinFilter,
) -> Option<(
    (
        (&ColumnIndex, &Arc<arrow::datatypes::Field>),
        (&ColumnIndex, &Arc<arrow::datatypes::Field>),
    ),
    (
        (&ColumnIndex, &Arc<arrow::datatypes::Field>),
        (&ColumnIndex, &Arc<arrow::datatypes::Field>),
    ),
)> {
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

    // dbg!(filter);

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

    x
}

#[cfg(test)]
mod tests {
    use crate::physical_planner::joins::utils::JoinFilter;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinSide;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::prelude::*;
    use std::sync::Arc;

    use crate::session_context::SeQuiLaSessionExt;

    const READS_PATH: &str = "../../testing/data/interval/reads.csv";
    const TARGETS_PATH: &str = "../../testing/data/interval/targets.csv";

    #[tokio::test]
    async fn init() -> datafusion::common::Result<()> {
        let schema = Schema::new(vec![
            Field::new("contig", DataType::Utf8, false),
            Field::new("pos_start", DataType::Int64, false),
            Field::new("pos_end", DataType::Int64, false),
        ]);

        let options = ConfigOptions::new();

        let config = SessionConfig::from(options)
            .with_batch_size(2)
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
            col("contig").alias("a1"),
            col("pos_start").alias("b1"),
            col("pos_end").alias("c1"),
        ];
        let target_renames = vec![
            col("contig").alias("a2"),
            col("pos_start").alias("b2"),
            col("pos_end").alias("c2"),
        ];

        let res = reads
            .select(reads_renames)?
            .join_on(
                targets.select(target_renames)?,
                JoinType::Inner,
                // a.column2 >= b.column1 AND a.column1 <= b.column2
                [
                    col("a1").eq(col("a2")),
                    col("b1").lt_eq(col("c2")),
                    // col("c2").gt(col("b1")),
                    col("c1").gt_eq(col("b2")), // col("b2").lt(col("c1"))
                ],
            )?
            .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))?;

        let x = res.clone().create_physical_plan().await?;
        // println!("execution_plan = {:?}", x);

        res.clone().explain(false, false)?.show().await?;

        // println!("\n------------------------\n");
        // println!("b={}", res.clone().collect().await?.len());
        // println!("---");
        res.show().await?;

        // select * from targets
        // join reads on
        // targets.contig = reads.contig
        // and targets.pos_start >= reads.pos_start
        // and targets.pos_end <= reads.pos_end;

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
            Arc::new(Column::new(rend, 3)),
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

        println!("{:#?}", x);

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

    fn parse_filter(filter: &JoinFilter) {}
}
