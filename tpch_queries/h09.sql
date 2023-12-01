SELECT nation,
       o_year,
       sum(amount) AS sum_profit
FROM
  (SELECT n_name AS nation,
          strftime('%Y', o_orderdate) AS o_year,
          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
   FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/part.parquet') part,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') supplier,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') lineitem,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/partsupp.parquet') partsupp,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') orders,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') nation
   WHERE s_suppkey = l_suppkey
     AND ps_suppkey = l_suppkey
     AND ps_partkey = l_partkey
     AND p_partkey = l_partkey
     AND o_orderkey = l_orderkey
     AND s_nationkey = n_nationkey
     AND p_name like '%green%' ) AS profit
GROUP BY nation,
         o_year
ORDER BY nation,
         o_year DESC ;
