SELECT ps_partkey,
       sum(ps_supplycost * ps_availqty) AS value
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/partsupp.parquet') partsupp,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') supplier,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') nation
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) >
  (SELECT sum(ps_supplycost * ps_availqty) * .0001 -- FRACTION = .0001/SF
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/partsupp.parquet') partsupp,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') supplier,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'GERMANY' )
ORDER BY value DESC
;
