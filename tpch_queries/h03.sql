SELECT l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate,
       o_shippriority
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') AS customer,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') AS orders,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') AS lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey,
         o_orderdate,
         o_shippriority
ORDER BY revenue DESC,
         o_orderdate
LIMIT 10
;
