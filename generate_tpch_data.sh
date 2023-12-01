#!/bin/bash

mkdir -p tpch_sf1/
duckdb ":memory:" << EOF
.bail on
.echo on
SELECT VERSION();
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);
COPY ( SELECT * FROM customer ) TO 'customer.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM nation ) TO 'nation.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM orders ) TO 'orders.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM part ) TO 'part.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM partsupp ) TO 'partsupp.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM region ) TO 'region.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM supplier ) TO 'supplier.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM lineitem ) TO 'lineitem.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
EOF
mv *.parquet tpch_sf1/

mkdir -p tpch_sf10
duckdb ":memory:" << EOF
.bail on
.echo on
SELECT VERSION();
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=10);
COPY ( SELECT * FROM customer ) TO 'customer.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM nation ) TO 'nation.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM orders ) TO 'orders.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM part ) TO 'part.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM partsupp ) TO 'partsupp.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM region ) TO 'region.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM supplier ) TO 'supplier.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
COPY ( SELECT * FROM lineitem ) TO 'lineitem.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
EOF
mv *.parquet tpch_sf10/


## NOTE: Do this on a bigger EC2 instance as even on this M3 Laptop with 36G mem, the lineitem crashed on OOM (duckdb 0.9.2)
# mkdir -p tpch_sf100
# duckdb tmp.duckdb << EOF
# .bail on
# .echo on
# SELECT VERSION();
# INSTALL tpch;
# LOAD tpch;
# CALL dbgen(sf=100);
# COPY ( SELECT * FROM customer ) TO 'customer.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM nation ) TO 'nation.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM orders ) TO 'orders.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM part ) TO 'part.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM partsupp ) TO 'partsupp.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM region ) TO 'region.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM supplier ) TO 'supplier.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# COPY ( SELECT * FROM lineitem ) TO 'lineitem.parquet' WITH ( FORMAT 'Parquet', COMPRESSION 'ZSTD' );
# EOF
# rm tmp.duckdb
# mv *.parquet tpch_sf100/