SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/part.parquet') part
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity <
    (SELECT 0.2 * avg(l_quantity)
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem
     WHERE l_partkey = p_partkey )
;
