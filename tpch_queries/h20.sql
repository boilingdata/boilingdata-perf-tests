SELECT s_name,
       s_address
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') supplier,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') nation
WHERE s_suppkey in
    (SELECT ps_suppkey
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/partsupp.parquet') partsupp
     WHERE ps_partkey in
         (SELECT p_partkey
          FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/part.parquet') part
          WHERE p_name like 'forest%' )
       AND ps_availqty >
         (SELECT 0.5 * sum(l_quantity)
          FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem
          WHERE l_partkey = ps_partkey
            AND l_suppkey = ps_suppkey
            AND l_shipdate >= '1994-01-01'
            AND l_shipdate < '1995-01-01' ) )
  AND s_nationkey = n_nationkey
  AND n_name = 'CANADA'
ORDER BY s_name
;
