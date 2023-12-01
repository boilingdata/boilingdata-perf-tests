SELECT c_custkey,
       c_name,
       sum(l_extendedprice * (1 - l_discount)) AS revenue,
       c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') customer,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') orders,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= '1993-10-01'
  AND o_orderdate < '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey,
         c_name,
         c_acctbal,
         c_phone,
         n_name,
         c_address,
         c_comment
ORDER BY revenue DESC
LIMIT 20 ;
