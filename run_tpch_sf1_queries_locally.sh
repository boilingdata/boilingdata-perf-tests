#!/bin/bash

for q in `ls -c1 tpch_queries`;
do
  echo "------------------- $q ------------------"
  cat tpch_queries/$q | gsed 's@s3://boilingdata-demo/@@g' | duckdb
done