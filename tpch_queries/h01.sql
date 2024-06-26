SELECT l_returnflag,
       l_linestatus,
       sum(l_quantity) AS sum_qty,
       sum(l_extendedprice) AS sum_base_price,
       sum(l_extendedprice*(1-l_discount)) AS sum_disc_price,
       sum(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,
       avg(l_quantity) AS avg_qty,
       avg(l_extendedprice) AS avg_price,
       avg(l_discount) AS avg_disc,
       count(*) AS count_order
FROM parquet_scan('s3://boilingdata-demo/tpch_sf1/lineitem.parquet') AS lineitem
WHERE l_shipdate <= '1998-09-02'  -- date '1998-12-01' - interval '[DELTA=90]' DAY
GROUP BY l_returnflag,
         l_linestatus
ORDER BY l_returnflag,
         l_linestatus;
