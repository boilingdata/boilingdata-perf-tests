SELECT o_orderpriority,
       count(*) AS order_count
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') AS orders
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < '1993-10-01'
  AND EXISTS
    (SELECT *
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') AS lineitem
     WHERE l_orderkey = o_orderkey
       AND l_commitdate < l_receiptdate )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
;
