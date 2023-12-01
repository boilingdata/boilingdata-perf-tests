SELECT c_count,
       count(*) AS custdist
FROM
  (SELECT c_custkey,
          count(o_orderkey) AS c_count
   FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/customer.parquet') customer
   LEFT OUTER JOIN parquet_scan('s3://boilingdata-demo/tpch_sf1/orders.parquet') orders ON c_custkey = o_custkey
   AND o_comment NOT LIKE '%special%requests%'
   GROUP BY c_custkey)
GROUP BY c_count
ORDER BY custdist DESC,
         c_count DESC
;
