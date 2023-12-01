SELECT s_name,
       count(*) AS numwait
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') supplier,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') l1,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') orders,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS
    (SELECT *
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') l2
     WHERE l2.l_orderkey = l1.l_orderkey
       AND l2.l_suppkey <> l1.l_suppkey )
  AND NOT EXISTS
    (SELECT *
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') l3
     WHERE l3.l_orderkey = l1.l_orderkey
       AND l3.l_suppkey <> l1.l_suppkey
       AND l3.l_receiptdate > l3.l_commitdate )
  AND s_nationkey = n_nationkey
  AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC,
         s_name
LIMIT 100
;
