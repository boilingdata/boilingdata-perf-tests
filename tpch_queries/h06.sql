SELECT sum(l_extendedprice * l_discount) AS revenue
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') AS lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
;
