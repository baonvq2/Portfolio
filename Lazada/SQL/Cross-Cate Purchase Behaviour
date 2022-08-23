DROP TABLE IF EXISTS user_retent_lm ;
 
CREATE TABLE user_retent_lm AS
SELECT  t1.utdid
        ,t1.cluster_name AS cluster00
        ,t2.cluster_name AS cluster01
        -- ,t1.month_
        -- ,t2.month_
FROM    (
            SELECT  utdid
                    ,cluster_name
                    ,month_
            FROM    1st_data_cluster
            WHERE   month_ <> DATETRUNC(GETDATE(), 'month')
        ) t1
LEFT JOIN (
              SELECT  utdid
                      ,cluster_name
                      ,month_
              FROM    traffic_d0 st2a
              LEFT JOIN traffic_info1 st2b
              ON      st2a.asc_sku_id = st2b.sku_id
              WHERE   month_ = DATEADD(DATETRUNC(GETDATE(), 'month'), - 1,'month')
          ) t2
ON      t1.utdid = t2.utdid
WHERE   t1.month_ <> t2.month_
;
 
DROP TABLE IF EXISTS cross_data_cluster ;
 
CREATE TABLE cross_data_cluster AS
SELECT  ipvuv
        ,ipv
        ,t1.cluster00
        ,CASE    WHEN t1.cluster01 NOT IN ('Uncategorized') AND t1.cluster01 IS NOT NULL THEN t1.cluster01
                 WHEN t1.cluster01 = 'Uncategorized' THEN 'Uncategorized' 
                 ELSE 'Churn rate/Lost' 
         END AS cluster01
        ,ipvuv/NULLIF(total_ipvuv,0) AS visit_share
        ,ipv/NULLIF(total_ipv,0) AS click_share
FROM    (
            SELECT  cluster00
                    ,cluster01
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    user_retent_lm
            GROUP BY cluster00
                     ,cluster01
        ) t1
LEFT JOIN (
              SELECT  cluster00
                      ,COUNT(DISTINCT utdid) AS total_ipvuv
                      ,COUNT(utdid) AS total_ipv
              FROM    user_retent_lm
              GROUP BY cluster00
          ) t2
ON      t1.cluster00 = t2.cluster00
WHERE   t1.cluster00 IS NOT NULL
AND     t1.cluster00 NOT IN ('Uncategorized')
;
 
--
DROP TABLE IF EXISTS user_retent_lm_cat1 ;
 
CREATE TABLE user_retent_lm_cat1 AS
SELECT  t1.utdid
        ,t1.regional_category1_name AS cat1_00
        ,t2.regional_category1_name AS cat1_01
        -- ,t1.month_
        -- ,t2.month_
FROM    (
            SELECT  utdid
                    ,regional_category1_name
                    ,month_
            FROM    1st_data_cat1
            WHERE   month_ <> DATETRUNC(GETDATE(), 'month')
        ) t1
LEFT JOIN (
              SELECT  utdid
                      ,regional_category1_name
                      ,month_
              FROM    traffic_d0 st2a
              LEFT JOIN traffic_info1 st2b
              ON      st2a.asc_sku_id = st2b.sku_id
              WHERE   month_ = DATEADD(DATETRUNC(GETDATE(), 'month'), - 1,'month')
          ) t2
ON      t1.utdid = t2.utdid
WHERE   t1.month_ <> t2.month_
;
 
DROP TABLE IF EXISTS cross_data_cat1 ;
 
CREATE TABLE cross_data_cat1 AS
SELECT  ipvuv
        ,ipv
        ,t1.cat1_00
        ,CASE    WHEN t1.cat1_01 NOT IN ('Uncategorized') AND t1.cat1_01 IS NOT NULL THEN t1.cat1_01
                 WHEN t1.cat1_01 = 'Uncategorized' THEN 'Uncategorized' 
                 ELSE 'Churn rate/Lost' 
         END AS cat1_01
        ,ipvuv/NULLIF(total_ipvuv,0) AS visit_share
        ,ipv/NULLIF(total_ipv,0) AS click_share
FROM    (
            SELECT  cat1_00
                    ,cat1_01
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    user_retent_lm_cat1
            GROUP BY cat1_00
                     ,cat1_01
        ) t1
LEFT JOIN (
              SELECT  cat1_00
                      ,COUNT(DISTINCT utdid) AS total_ipvuv
                      ,COUNT(utdid) AS total_ipv
              FROM    user_retent_lm_cat1
              GROUP BY cat1_00
          ) t2
ON      t1.cat1_00 = t2.cat1_00
WHERE   t1.cat1_00 IS NOT NULL
AND     t1.cat1_00 NOT IN ('Uncategorized')
;
-- SELECT *
-- FROM cross_data_cluster;
 
DROP TABLE IF EXISTS user_retent_lm_w ;
 
CREATE TABLE user_retent_lm_w AS
SELECT  t1.utdid
        ,t1.cluster_name AS cluster00
        ,t2.cluster_name AS cluster01
FROM    (
            SELECT  utdid
                    ,cluster_name
                    ,week_
            FROM    1st_data_cluster_w
            WHERE   week_ = WEEKOFYEAR(GETDATE()) - 5
        ) t1
LEFT JOIN (
              SELECT  utdid
                      ,cluster_name
                      ,week_
              FROM    traffic_d0_w st2a
              LEFT JOIN traffic_info1_w st2b
              ON      st2a.asc_sku_id = st2b.sku_id
              WHERE   week_ = WEEKOFYEAR(GETDATE()) - 1
          ) t2
ON      t1.utdid = t2.utdid
;
 
DROP TABLE IF EXISTS cross_data_cluster_w ;
 
CREATE TABLE cross_data_cluster_w AS
SELECT  ipvuv
        ,ipv
        ,t1.cluster00
        ,CASE    WHEN t1.cluster01 NOT IN ('Uncategorized') AND t1.cluster01 IS NOT NULL THEN t1.cluster01
                 WHEN t1.cluster01 = 'Uncategorized' THEN 'Uncategorized' 
                 ELSE 'Churn rate/Lost' 
         END AS cluster01
        ,ipvuv/NULLIF(total_ipvuv,0) AS visit_share
        ,ipv/NULLIF(total_ipv,0) AS click_share
FROM    (
            SELECT  cluster00
                    ,cluster01
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    user_retent_lm_w
            GROUP BY cluster00
                     ,cluster01
        ) t1
LEFT JOIN (
              SELECT  cluster00
                      ,COUNT(DISTINCT utdid) AS total_ipvuv
                      ,COUNT(utdid) AS total_ipv
              FROM    user_retent_lm_w
              GROUP BY cluster00
          ) t2
ON      t1.cluster00 = t2.cluster00
WHERE   t1.cluster00 IS NOT NULL
AND     t1.cluster00 NOT IN ('Uncategorized')
;
DROP TABLE IF EXISTS user_retent_lm_cat1_w ;
 
CREATE TABLE user_retent_lm_cat1_w AS
SELECT  t1.utdid
        ,t1.regional_category1_name AS cat1_00
        ,t2.regional_category1_name AS cat1_01
FROM    (
            SELECT  utdid
                    ,regional_category1_name
                    ,week_
            FROM    1st_data_cat1_w
            WHERE   week_ = WEEKOFYEAR(GETDATE()) - 5
        ) t1
LEFT JOIN (
              SELECT  utdid
                      ,regional_category1_name
                      ,week_
              FROM    traffic_d0_w st2a
              LEFT JOIN traffic_info1_w st2b
              ON      st2a.asc_sku_id = st2b.sku_id
              WHERE   week_ = WEEKOFYEAR(GETDATE()) - 1
          ) t2
ON      t1.utdid = t2.utdid
;
 
DROP TABLE IF EXISTS cross_data_cat1_w ;
 
CREATE TABLE cross_data_cat1_w AS
SELECT  ipvuv
        ,ipv
        ,t1.cat1_00
        ,CASE    WHEN t1.cat1_01 NOT IN ('Uncategorized') AND t1.cat1_01 IS NOT NULL THEN t1.cat1_01
                 WHEN t1.cat1_01 = 'Uncategorized' THEN 'Uncategorized' 
                 ELSE 'Churn rate/Lost' 
         END AS cat1_01
        ,ipvuv/NULLIF(total_ipvuv,0) AS visit_share
        ,ipv/NULLIF(total_ipv,0) AS click_share
FROM    (
            SELECT  cat1_00
                    ,cat1_01
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    user_retent_lm_cat1_w
            GROUP BY cat1_00
                     ,cat1_01
        ) t1
LEFT JOIN (
              SELECT  cat1_00
                      ,COUNT(DISTINCT utdid) AS total_ipvuv
                      ,COUNT(utdid) AS total_ipv
              FROM    user_retent_lm_cat1_w
              GROUP BY cat1_00
          ) t2
ON      t1.cat1_00 = t2.cat1_00
WHERE   t1.cat1_00 IS NOT NULL
AND     t1.cat1_00 NOT IN ('Uncategorized')
;
