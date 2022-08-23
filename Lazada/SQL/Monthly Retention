DROP TABLE IF EXISTS traffic_info1 ;
 
CREATE TABLE traffic_info1 AS
SELECT  DISTINCT sku_id
        ,business_type
        ,business_type_level2
        ,CASE    WHEN regional_category1_name IN ("Digital Goods","Digital Utilities","Service Product","Services","Special Digital Products") THEN 'DG'
                 WHEN regional_category1_name IN ("Audio","Cameras","Cameras & Drones","Computers & Laptops","Data Storage","Home Appliances","Large Appliances","Mobiles & Tablets","Monitors & Printers","Small Appliances","Smart Devices","TV, Audio / Video, Gaming & Wearables","Televisions & Videos","Electronics Accessories") THEN 'EL'
                 WHEN regional_category1_name IN ("Bags and Travel","Fashion","Men's Shoes and Clothing","Sports & Outdoors","Watches Sunglasses Jewellery","Women's Shoes and Clothing","Sports Shoes and Clothing") THEN 'FA'
                 WHEN regional_category1_name IN ("Groceries","Household Supplies","Pet Supplies") THEN 'GC'
                 WHEN regional_category1_name IN ("Bedding & Bath","Furniture & Décor","Furniture & Organization","Kitchen & Dining","Laundry & Cleaning","Laundry & Cleaning Equipment","Lighting & Décor","Media, Music & Books","Motors","Outdoor & Garden","Stationery & Craft","Tools & Home Improvement","Tools, DIY & Outdoor") THEN 'GM'
                 WHEN regional_category1_name IN ("Beauty","Health","Health & Beauty") THEN 'HB'
                 WHEN regional_category1_name IN ("Mother & Baby","Toys & Games") THEN 'MB' 
                 ELSE 'Uncategorized' 
         END AS cluster_name
        ,product_name
        ,seller_name
        ,seller_id
        ,seller_short_code
        ,commercial_category_name
        ,regional_category1_name
        ,regional_category2_name
        ,regional_category3_name
        ,industry_name
        ,category_id
FROM    lazada_cdm.dim_lzd_prd_sku_core_vn
WHERE   venture = 'VN'
AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
;
 
--rr--
DROP TABLE IF EXISTS traffic_d0 ;
 
CREATE TABLE traffic_d0 AS
SELECT  DISTINCT utdid
                    ,TO_DATE(SUBSTR(visit_date,1,6),'yyyymm') AS month_
                    ,asc_sku_id
            FROM    alilog.dwd_lzd_log_ut_pv_di
            WHERE   1 = 1
            AND     ds <= TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
            AND     ds >= TO_CHAR(DATETRUNC(GETDATE(), 'yyyy'), 'yyyymmdd')
            AND     app_id IN ('23867946@aliyunos','23867946@android','23868882@ipad','23868882@iphoneos')
            AND     venture = 'VN'
            AND     url_type = 'ipv'
;
 
DROP TABLE IF EXISTS overall_app_rr ;
 
CREATE TABLE overall_app_rr AS
SELECT  t4.month_num
        ,CASE    WHEN t4.monthdiff BETWEEN 0 AND 9 THEN CONCAT('month0',monthdiff) 
                 ELSE CONCAT('month',monthdiff) 
         END AS co_month
        ,retent_month
        ,t4.ipvuv
        ,t4.ipv
        ,t4.ipvuv/nullif(t5.ipvuv,0) AS overall_rr
        ,t4.ipvuv/nullif(
            (
                SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ) st5 WHERE month_ = '2021-01-01 00:00:00' GROUP BY month_
            )
            ,0
        ) AS retention_share
FROM    (
            SELECT  month_num
                    ,monthdiff
                    ,DATEADD(month_num,monthdiff,'mm') AS retent_month
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    (
                        SELECT  t1.utdid
                                ,DATEDIFF(t2.month_,t1.month_,'mm') AS monthdiff
                                ,t1.month_ AS month_num
                        FROM    (
                                    SELECT  utdid
                                            ,MIN(month_) AS month_
                                    FROM    traffic_d0
                                    GROUP BY utdid
                                ) t1 INNER
                        JOIN    traffic_d0 t2
                        ON      t1.utdid = t2.utdid
                    ) t3
            WHERE   1 = 1
            GROUP BY month_num
                     ,monthdiff
        ) t4
LEFT JOIN (
              SELECT  month_
                      ,COUNT(DISTINCT utdid) AS ipvuv
              FROM    (
                          SELECT  utdid
                                  ,MIN(month_) AS month_
                          FROM    traffic_d0
                          GROUP BY utdid
                      ) st5
              GROUP BY month_
          ) t5
ON      t4.month_num = t5.month_ 
;
DROP TABLE IF EXISTS 1st_data_cluster ;
 
CREATE TABLE 1st_data_cluster AS
SELECT  utdid
        ,cluster_name
        ,MIN(month_) AS month_
FROM    traffic_d0 t1
LEFT JOIN traffic_info1 t2
ON      t1.asc_sku_id = t2.sku_id
GROUP BY utdid
         ,cluster_name
;
DROP TABLE IF EXISTS 1st_data_cat1 ;
 
CREATE TABLE 1st_data_cat1 AS
SELECT  utdid
        ,cluster_name
        ,regional_category1_name
        ,MIN(month_) AS month_
FROM    traffic_d0 t1
LEFT JOIN traffic_info1 t2
ON      t1.asc_sku_id = t2.sku_id
GROUP BY utdid
         ,cluster_name
         ,regional_category1_name
;
DROP TABLE IF EXISTS 1st_data_comcat ;
 
CREATE TABLE 1st_data_comcat AS
SELECT  utdid
        ,commercial_category_name
        ,MIN(month_) AS month_
FROM    traffic_d0 t1
LEFT JOIN traffic_info1 t2
ON      t1.asc_sku_id = t2.sku_id
GROUP BY utdid
         ,commercial_category_name
;
DROP TABLE IF EXISTS retent_data_cluster ;
 
CREATE TABLE retent_data_cluster AS
SELECT  t1.utdid
        ,t1.cluster_name
        ,DATEDIFF(t2.month_,t1.month_,'mm') AS monthdiff
        ,t1.month_ AS month_num
FROM    1st_data_cluster t1 INNER
JOIN    (
            SELECT  DISTINCT utdid
                    ,month_
            FROM    traffic_d0
            WHERE   utdid IN (SELECT utdid FROM 1st_data_cluster)
        ) t2
ON      t1.utdid = t2.utdid
;
DROP TABLE IF EXISTS retent_data_cat1 ;
 
CREATE TABLE retent_data_cat1 AS
SELECT  t1.utdid
        ,t1.cluster_name
        ,t1.regional_category1_name
        ,DATEDIFF(t2.month_,t1.month_,'mm') AS monthdiff
        ,t1.month_ AS month_num
FROM    1st_data_cat1 t1 INNER
JOIN    (
            SELECT  DISTINCT utdid
                    ,month_
            FROM    traffic_d0
            WHERE   utdid IN (SELECT utdid FROM 1st_data_cat1)
        ) t2
ON      t1.utdid = t2.utdid
;
 
DROP TABLE IF EXISTS retent_data_comcat ;
 
CREATE TABLE retent_data_comcat AS
SELECT  t1.utdid
        ,t1.commercial_category_name
        ,DATEDIFF(t2.month_,t1.month_,'mm') AS monthdiff
        ,t1.month_ AS month_num
FROM    1st_data_comcat t1 INNER
JOIN    (
            SELECT  DISTINCT utdid
                    ,month_
            FROM    traffic_d0
            WHERE   utdid IN (SELECT utdid FROM 1st_data_comcat)
        ) t2
ON      t1.utdid = t2.utdid
;
DROP TABLE IF EXISTS cluster_app_rr ;
 
CREATE TABLE cluster_app_rr AS
SELECT  t4.month_num
        ,t4.cluster_name
        ,retent_month
        ,CASE    WHEN t4.monthdiff BETWEEN 0 AND 9 THEN CONCAT('month0',monthdiff) 
                 ELSE CONCAT('month',monthdiff) 
         END AS co_month
        ,t4.ipvuv
        ,t4.ipv
        ,t4.ipvuv/nullif(t5.ipvuv,0) AS overall_rr
        ,CASE    WHEN t4.cluster_name = 'EL' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0)
                 WHEN t4.cluster_name = 'GM' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0)
                 WHEN t4.cluster_name = 'FA' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0)
                 WHEN t4.cluster_name = 'HB' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'HB' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0)
                 WHEN t4.cluster_name = 'GC' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GC' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0)
                 WHEN t4.cluster_name = 'MB' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'MB' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0)
                 WHEN t4.cluster_name = 'DG' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'DG' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name) ,0) 
         END AS retention_share
FROM    (
            SELECT  month_num
                    ,monthdiff
                    ,DATEADD(month_num,monthdiff,'mm') AS retent_month
                    ,cluster_name
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    retent_data_cluster t3
            WHERE   1 = 1
            AND     monthdiff IS NOT NULL
            AND     monthdiff >= 0
            GROUP BY month_num
                     ,monthdiff
                     ,cluster_name
        ) t4
LEFT JOIN (
              SELECT  month_
                      ,cluster_name
                      ,COUNT(DISTINCT utdid) AS ipvuv
              FROM    1st_data_cluster
              GROUP BY month_
                       ,cluster_name
          ) t5
ON      (t4.month_num = t5.month_ AND t4.cluster_name = t5.cluster_name)
;
 
DROP TABLE IF EXISTS cat1_app_rr ;
 
CREATE TABLE cat1_app_rr AS
SELECT  t4.month_num
        ,t4.cluster_name
        ,retent_month
        ,t4.regional_category1_name
        ,CASE    WHEN t4.monthdiff BETWEEN 0 AND 9 THEN CONCAT('month0',monthdiff) 
                 ELSE CONCAT('month',monthdiff) 
         END AS co_month
        ,t4.ipvuv
        ,t4.ipv
        ,t4.ipvuv/nullif(t5.ipvuv,0) AS overall_rr
        ,CASE    WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Mobiles & Tablets') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Mobiles & Tablets' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Computers & Laptops') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Computers & Laptops' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Televisions & Videos') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Televisions & Videos' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Cameras & Drones') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Cameras & Drones' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Small Appliances') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Small Appliances' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Large Appliances') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Large Appliances' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Monitors & Printers') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Monitors & Printers' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Data Storage') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Data Storage' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Audio') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Audio' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Smart Devices') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Smart Devices' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'EL' AND t4.regional_category1_name = 'Electronics Accessories') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'EL' AND regional_category1_name = 'Electronics Accessories' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Motors') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Motors' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Media, Music & Books') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Media, Music & Books' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Bedding & Bath') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Bedding & Bath' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Furniture & Organization') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Furniture & Organization' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Kitchen & Dining') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Kitchen & Dining' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Laundry & Cleaning Equipment') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Laundry & Cleaning Equipment' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Tools & Home Improvement') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Tools & Home Improvement' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Stationery & Craft') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Stationery & Craft' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Outdoor & Garden') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Outdoor & Garden' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GM' AND t4.regional_category1_name = 'Lighting & Décor') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GM' AND regional_category1_name = 'Lighting & Décor' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'FA' AND t4.regional_category1_name = 'Bags and Travel') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND regional_category1_name = 'Bags and Travel' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'FA' AND t4.regional_category1_name IN ("Women's Shoes and Clothing")) THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND regional_category1_name IN ("Women's Shoes and Clothing") AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'FA' AND t4.regional_category1_name = 'Watches Sunglasses Jewellery') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND regional_category1_name = 'Watches Sunglasses Jewellery' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'FA' AND t4.regional_category1_name IN ("Men's Shoes and Clothing")) THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND regional_category1_name IN ("Men's Shoes and Clothing") AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'FA' AND t4.regional_category1_name = 'Sports Shoes and Clothing') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND regional_category1_name = 'Sports Shoes and Clothing' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'FA' AND t4.regional_category1_name = 'Sports & Outdoors') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'FA' AND regional_category1_name = 'Sports & Outdoors' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'HB' AND t4.regional_category1_name = 'Beauty') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'HB' AND regional_category1_name = 'Beauty' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'HB' AND t4.regional_category1_name = 'Health') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'HB' AND regional_category1_name = 'Health' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GC' AND t4.regional_category1_name = 'Groceries') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GC' AND regional_category1_name = 'Groceries' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GC' AND t4.regional_category1_name = 'Pet Supplies') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GC' AND regional_category1_name = 'Pet Supplies' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'GC' AND t4.regional_category1_name = 'Household Supplies') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'GC' AND regional_category1_name = 'Household Supplies' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'MB' AND t4.regional_category1_name = 'Mother & Baby') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'MB' AND regional_category1_name = 'Mother & Baby' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'MB' AND t4.regional_category1_name = 'Toys & Games') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'MB' AND regional_category1_name = 'Toys & Games' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'DG' AND t4.regional_category1_name = 'Digital Goods') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'DG' AND regional_category1_name = 'Digital Goods' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'DG' AND t4.regional_category1_name = 'Services') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'DG' AND regional_category1_name = 'Services' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'DG' AND t4.regional_category1_name = 'Service Product') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'DG' AND regional_category1_name = 'Service Product' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
        WHEN (t4.cluster_name = 'DG' AND t4.regional_category1_name = 'Digital Utilities') THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE cluster_name = 'DG' AND regional_category1_name = 'Digital Utilities' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,cluster_name ,regional_category1_name) ,0)
         END AS retention_share
FROM    (
            SELECT  month_num
                    ,monthdiff
                    ,DATEADD(month_num,monthdiff,'mm') AS retent_month
                    ,cluster_name
                    ,regional_category1_name
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    retent_data_cat1 t3
            WHERE   1 = 1
            AND     monthdiff IS NOT NULL
            AND     monthdiff >= 0
            GROUP BY month_num
                     ,monthdiff
                     ,cluster_name
                     ,regional_category1_name
        ) t4
LEFT JOIN (
              SELECT  month_
                      ,cluster_name
                      ,regional_category1_name
                      ,COUNT(DISTINCT utdid) AS ipvuv
              FROM    1st_data_cat1
              GROUP BY month_
                       ,cluster_name
                       ,regional_category1_name
          ) t5
ON      (
                t4.month_num = t5.month_
            AND t4.cluster_name = t5.cluster_name
            AND t4.regional_category1_name = t5.regional_category1_name
        )
;
 
DROP TABLE IF EXISTS comcat_app_rr ;
 
CREATE TABLE comcat_app_rr AS
SELECT  t4.month_num
        ,t4.commercial_category_name
        ,retent_month
        ,CASE    WHEN t4.monthdiff BETWEEN 0 AND 9 THEN CONCAT('month0',monthdiff) 
                 ELSE CONCAT('month',monthdiff) 
         END AS co_month
        ,t4.ipvuv
        ,t4.ipv
        ,t4.ipvuv/nullif(t5.ipvuv,0) AS overall_rr
        ,CASE    WHEN t4.commercial_category_name = 'Accessories' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Accessories' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Bags & Travel' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Bags & Travel' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Cameras Devices' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Cameras Devices' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Computers & Laptops Devices' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Computers & Laptops Devices' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Digital' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Digital' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Fashion' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Fashion' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Groceries' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Groceries' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Health & Beauty' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Health & Beauty' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Home & Living' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Home & Living' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Home Appliances' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Home Appliances' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Media, Music & Books' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Media, Music & Books' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Mobiles & Tablets Devices' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Mobiles & Tablets Devices' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Mother & Baby' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Mother & Baby' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Motors' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Motors' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Pet Supplies' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Pet Supplies' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Sports & Outdoors' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Sports & Outdoors' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Sportswear' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Sportswear' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Toys & Games' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Toys & Games' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Uncategorized' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Uncategorized' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0)
                 WHEN t4.commercial_category_name = 'Watches Sunglasses Jewellery' THEN t4.ipvuv/nullif( ( SELECT COUNT(DISTINCT utdid) AS ipvuv FROM ( SELECT utdid ,asc_sku_id ,MIN(month_) AS month_ FROM traffic_d0 GROUP BY utdid ,asc_sku_id ) a LEFT JOIN traffic_info1 b ON a.asc_sku_id = b.sku_id WHERE commercial_category_name = 'Watches Sunglasses Jewellery' AND month_ = '2021-01-01 00:00:00' GROUP BY month_ ,commercial_category_name) ,0) 
         END AS retention_share
FROM    (
            SELECT  month_num
                    ,monthdiff
                    ,DATEADD(month_num,monthdiff,'mm') AS retent_month
                    ,commercial_category_name
                    ,COUNT(DISTINCT utdid) AS ipvuv
                    ,COUNT(utdid) AS ipv
            FROM    retent_data_comcat t3
            WHERE   1 = 1
            AND     monthdiff IS NOT NULL
            AND     monthdiff >= 0
            GROUP BY month_num
                     ,monthdiff
                     ,commercial_category_name
        ) t4
LEFT JOIN (
              SELECT  month_
                      ,commercial_category_name
                      ,COUNT(DISTINCT utdid) AS ipvuv
              FROM    1st_data_comcat
              GROUP BY month_
                       ,commercial_category_name
          ) t5
ON      (
                t4.month_num = t5.month_
            AND t4.commercial_category_name = t5.commercial_category_name
        )
;
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
                --  WHEN t1.cluster01 = 'GM' THEN 'GM'
                --  WHEN t1.cluster01 = 'FA' THEN 'FA'
                --  WHEN t1.cluster01 = 'HB' THEN 'HB'
                --  WHEN t1.cluster01 = 'GC' THEN 'GC'
                --  WHEN t1.cluster01 = 'MB' THEN 'MB'
                --  WHEN t1.cluster01 = 'DG' THEN 'DG'
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
ALTER TABLE traffic_info1 SET LIFECYCLE 365 ;
ALTER TABLE traffic_d0 SET LIFECYCLE 365 ;
ALTER TABLE overall_app_rr SET LIFECYCLE 365 ;
ALTER TABLE 1st_data_cluster SET LIFECYCLE 365 ;
ALTER TABLE 1st_data_cat1 SET LIFECYCLE 365 ;
ALTER TABLE 1st_data_comcat SET LIFECYCLE 365 ;
ALTER TABLE retent_data_cluster SET LIFECYCLE 365 ;
ALTER TABLE retent_data_cat1 SET LIFECYCLE 365 ;
ALTER TABLE retent_data_comcat SET LIFECYCLE 365 ;
ALTER TABLE cluster_app_rr SET LIFECYCLE 365 ;
ALTER TABLE cat1_app_rr SET LIFECYCLE 365 ;
ALTER TABLE comcat_app_rr SET LIFECYCLE 365 ;
ALTER TABLE user_retent_lm SET LIFECYCLE 365 ;
ALTER TABLE cross_data_cluster SET LIFECYCLE 365 ;
ALTER TABLE user_retent_lm_cat1 SET LIFECYCLE 365 ;
ALTER TABLE cross_data_cat1 SET LIFECYCLE 365 ;

