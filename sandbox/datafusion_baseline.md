# Update 22.07.2024

## SeQuiLa
```
+---------+
| count(1)|
+---------+
|154374873|
+---------+

Time taken: 32912 ms

```

## Duckdb
```
.timer on
PRAGMA threads=1;
PRAGMA prefer_range_joins=true;
D SELECT count(*) FROM read_csv('/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainRn4_chr1.csv') a, read_csv('/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainVicPac2_chr1.csv') b where (a.column0=b.column0 and a.column2>=b.column1 and a.column1<=b.column2);
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│    154374873 │
└──────────────┘
Run Time (s): real 1.967 user 1.948447 sys 0.015313



SELECT count(*) 
FROM 
      read_parquet('/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainVicPac2.parquet/*.parquet') a, 
      read_parquet('/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainRn4.parquet/*.parquet') b 
WHERE (a.column0=b.column0 and a.column2>=b.column1 and a.column1<=b.column2);
```

## SeQuiLa-native
```
set datafusion.execution.target_partitions=1;
set sequila.prefer_interval_join to true;
set sequila.interval_join_algorithm to 'coitrees';

CREATE EXTERNAL TABLE chainRn4_chr1
STORED AS CSV
LOCATION '/Users/mwiewior/research/data/AIListTestData/sequila-native/chainRn4.bed'
OPTIONS ('has_header' 'true', 'format.delimiter' '\t' );

CREATE EXTERNAL TABLE chainVicPac2_chr1
STORED AS CSV
LOCATION '/Users/mwiewior/research/data/AIListTestData/sequila-native/chainVicPac2.bed'
OPTIONS ('has_header' 'true', 'format.delimiter' '\t' );

select count(*) from `chainRn4_chr1` a, `chainVicPac2_chr1` b where (a.column0=b.column0 and a.column2>=b.column1 and a.column1<=b.column2);

[2024-07-22T17:28:36Z INFO  sequila_core::physical_planner::sequila_physical_planner] sequila_config: SequilaConfig { prefer_interval_join: true, interval_join_algorithm: Coitrees }
+-----------+
| COUNT(*)  |
+-----------+
| 154374873 |
+-----------+
1 row(s) fetched. 
Elapsed 2.457 seconds.


[2024-07-22T17:29:32Z INFO  sequila_core::physical_planner::sequila_physical_planner] sequila_config: SequilaConfig { prefer_interval_join: true, interval_join_algorithm: ArrayIntervalTree }
+-----------+
| COUNT(*)  |
+-----------+
| 154374873 |
+-----------+
1 row(s) fetched. 
Elapsed 2.705 seconds.
```



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
LOCATION '/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainXenTro3Link.parquet';
       
CREATE EXTERNAL TABLE test2
STORED AS PARQUET
LOCATION '/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainXenTro3Link.parquet';

explain select count(*) from test a, test2 b where (a._c0=b._c0 and a._c2>=b._c1 and a._c1<=b._c2);
```

```

CREATE EXTERNAL TABLE chainVicPac2
STORED AS PARQUET
LOCATION '/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainVicPac2.parquet';
       
CREATE EXTERNAL TABLE chainRn4
STORED AS PARQUET
LOCATION '/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainRn4.parquet';       


set datafusion.execution.target_partitions=8;
set sequila.prefer_interval_join to true;
set sequila.interval_join_algorithm to 'IntervalTree';
set datafusion.optimizer.repartition_joins to false;   
```