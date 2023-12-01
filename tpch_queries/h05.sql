SELECT n_name,
       sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') AS customer,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') AS orders,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') AS lineitem,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') AS supplier,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') AS nation,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/region.parquet') AS region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= '1994-01-01'
  AND o_orderdate < '1995-01-01'
GROUP BY n_name
ORDER BY revenue DESC
;
