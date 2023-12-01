SELECT c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity) AS sum_qty
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') customer,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') orders,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem
WHERE o_orderkey in
    (SELECT l_orderkey
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem
     GROUP BY l_orderkey
     HAVING sum(l_quantity) > 300)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC,
         o_orderdate
LIMIT 100
;
