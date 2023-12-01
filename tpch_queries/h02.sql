SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/part.parquet') AS part,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') AS supplier,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/partsupp.parquet') AS partsupp,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') AS nation,
     parquet_scan('s3://boilingdata-demo/tpch_sf1/region.parquet') AS region,
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 25 -- [SIZE]
  AND p_type like '%BRASS' -- '%[TYPE]'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE' -- '[REGION]'
  AND ps_supplycost =
    (SELECT min(ps_supplycost)
     FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/partsupp.parquet') AS partsupp,
          parquet_scan('s3://boilingdata-demo/tpch_sf1/supplier.parquet') AS supplier,
          parquet_scan('s3://boilingdata-demo/tpch_sf1/nation.parquet') AS nation,
          parquet_scan('s3://boilingdata-demo/tpch_sf1/region.parquet') AS region
     WHERE p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND s_nationkey = n_nationkey
       AND n_regionkey = r_regionkey
       AND r_name = 'EUROPE') -- '[REGION]' )
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey
LIMIT 100
;
