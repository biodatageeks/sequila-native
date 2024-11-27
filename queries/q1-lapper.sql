SET sequila.prefer_interval_join TO true;
SET sequila.interval_join_algorithm TO lapper;
SET datafusion.optimizer.repartition_joins TO false;

CREATE EXTERNAL TABLE a (contig VARCHAR NOT NULL, start BIGINT NOT NULL, end BIGINT NOT NULL)
STORED AS CSV
LOCATION './testing/data/exons.bed'
OPTIONS ('delimiter' '\t', 'has_header' 'false');

CREATE EXTERNAL TABLE b (contig VARCHAR NOT NULL, start BIGINT NOT NULL, end BIGINT NOT NULL)
STORED AS CSV
LOCATION './testing/data/fBrain-DS14718.bed'
OPTIONS ('delimiter' '\t', 'has_header' 'false');

select count(1) from a join b
 on a.contig = b.contig
and a.end >= b.start
and a.start <= b.end;
