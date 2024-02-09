 ## Only chr1
```
set datafusion.execution.target_partitions=1;

+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| plan_type     | plan                                                                                                                                                                   |
+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| logical_plan  | Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1)) AS COUNT(*)]]                                                                                                          |
|               |   Projection:                                                                                                                                                          |
|               |     Inner Join: a.column0 = b.column0 Filter: a.column2 >= b.column1 AND a.column1 <= b.column2                                                                        |
|               |       SubqueryAlias: a                                                                                                                                                 |
|               |         TableScan: test_1 projection=[column0, column1, column2]                                                                                                       |
|               |       SubqueryAlias: b                                                                                                                                                 |
|               |         TableScan: test_2 projection=[column0, column1, column2]                                                                                                       |
| physical_plan | AggregateExec: mode=Single, gby=[], aggr=[COUNT(*)]                                                                                                                    |
|               |   ProjectionExec: expr=[]                                                                                                                                              |
|               |     CoalesceBatchesExec: target_batch_size=8192                                                                                                                        |
|               |       HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(column0@0, column0@0)], filter=column2@1 >= column1@2 AND column1@0 <= column2@3                           |
|               |         CsvExec: file_groups={1 group: [[Users/mwiewior/research/data/AIListTestData/chainRn4_chr1.csv]]}, projection=[column0, column1, column2], has_header=true     |
|               |         CsvExec: file_groups={1 group: [[Users/mwiewior/research/data/AIListTestData/chainVicPac2_chr1.csv]]}, projection=[column0, column1, column2], has_header=true |
|               |                                                                                                                                                                        |
+---------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
## Results
Interrupted after 40min. No results.
Huge memory consumption at peak around 70GB.
Potential reason:
Inner Join: a.column0 = b.column0 Filter: a.column2 >= b.column1 AND a.column1 <= b.column2
so the join is only on the first column, but the filter is on the second and third column. This is a very inefficient join.
>

```sql

CREATE EXTERNAL TABLE test
STORED AS PARQUET
LOCATION '/Users/mwiewior/research/data/AIListTestData/chainXenTro3Link.parquet';
       
CREATE EXTERNAL TABLE test2
STORED AS PARQUET
LOCATION '/Users/mwiewior/research/data/AIListTestData/chainXenTro3Link.parquet';

explain select count(*) from test a join test2 b on a._c0=b._c0;
```