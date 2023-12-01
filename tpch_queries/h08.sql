SELECT o_year,
       sum(CASE
               WHEN nation = 'BRAZIL' THEN volume
               ELSE 0
           END) / sum(volume) AS mkt_share
FROM
  (SELECT strftime('%Y', o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount) AS volume,
          n2.n_name AS nation
   FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/part.parquet') part,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') supplier,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') orders,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') customer,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') n1,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') n2,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/region.parquet') region
   WHERE p_partkey = l_partkey
     AND s_suppkey = l_suppkey
     AND l_orderkey = o_orderkey
     AND o_custkey = c_custkey
     AND c_nationkey = n1.n_nationkey
     AND n1.n_regionkey = r_regionkey
     AND r_name = 'AMERICA'
     AND s_nationkey = n2.n_nationkey
     AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
     AND p_type = 'ECONOMY ANODIZED STEEL' ) AS all_nations
GROUP BY o_year
ORDER BY o_year
;
