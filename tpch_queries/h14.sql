SELECT 100.00 * sum(CASE
                        WHEN p_type like 'PROMO%' THEN l_extendedprice*(1-l_discount)
                        ELSE 0
                    END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/part.parquet') part
WHERE l_partkey = p_partkey
  AND l_shipdate >= '1995-09-01'
  AND l_shipdate < '1995-10-01'
;
