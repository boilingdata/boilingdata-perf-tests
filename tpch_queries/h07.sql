SELECT supp_nation,
       cust_nation,
       l_year,
       sum(volume) AS revenue
FROM
  (SELECT n1.n_name AS supp_nation,
          n2.n_name AS cust_nation,
          strftime('%Y', l_shipdate) AS l_year,
          l_extendedprice * (1 - l_discount) AS volume
   FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') AS supplier,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') AS lineitem,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') AS orders,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') AS customer,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') n1,
        parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') n2
   WHERE s_suppkey = l_suppkey
     AND o_orderkey = l_orderkey
     AND c_custkey = o_custkey
     AND s_nationkey = n1.n_nationkey
     AND c_nationkey = n2.n_nationkey
     AND ((n1.n_name = 'FRANCE'
           AND n2.n_name = 'GERMANY')
          OR (n1.n_name = 'GERMANY'
              AND n2.n_name = 'FRANCE'))
     AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31' ) AS shipping
GROUP BY supp_nation,
         cust_nation,
         l_year
ORDER BY supp_nation,
         cust_nation,
         l_year
;
