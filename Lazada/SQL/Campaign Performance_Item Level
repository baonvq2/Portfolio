----------------------------------
--info--
----------------------------------
DROP TABLE IF EXISTS item_info_bao_tet ;
 
CREATE TABLE item_info_bao_tet AS
SELECT  t1.product_id
        ,business_type
        ,business_type_level2
        ,cluster_name
        ,product_name
        ,seller_name
        ,seller_id
        ,seller_short_code
        ,commercial_category_name
        ,regional_category1_name
        ,regional_category2_name
        ,regional_category3_name
FROM    (
            SELECT  DISTINCT product_id
                    ,business_type
                    ,business_type_level2
                    ,CASE    WHEN regional_category1_name IN ("Digital Goods","Digital Utilities","Service Product","Services","Special Digital Products") THEN 'DG'
                             WHEN regional_category1_name IN ("Audio","Cameras","Cameras & Drones","Computers & Laptops","Data Storage","Home Appliances","Large Appliances","Mobiles & Tablets","Monitors & Printers","Small Appliances","Smart Devices","TV, Audio / Video, Gaming & Wearables","Televisions & Videos") THEN 'EL'
                             WHEN regional_category1_name IN ("Bags and Travel","Fashion","Men's Shoes and Clothing","Sports & Outdoors","Watches Sunglasses Jewellery","Women's Shoes and Clothing") THEN 'FA'
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
            FROM    lazada_cdm.dim_lzd_prd_sku_core_vn
            WHERE   venture = 'VN'
            AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
        ) t1
;
----------------------------------
--a2c_unit--
----------------------------------
DROP TABLE IF EXISTS item_a2c_unit_bao_tet ;
 
CREATE TABLE item_a2c_unit_bao_tet AS
SELECT  t1.product_id
        ,total_a2c_unit
        ,a2c_unit_1231
        ,a2c_unit_0101
        ,a2c_unit_0102
        ,a2c_unit_0103
        ,a2c_unit_0104
        ,a2c_unit_0105
        ,a2c_unit_0106
        ,a2c_unit_0107
        ,a2c_unit_0108
        ,a2c_unit_0109
        ,a2c_unit_0110
        ,a2c_unit_0111
        ,a2c_unit_0112
FROM    (
            SELECT  product_id
                    ,SUM(
                        CASE    WHEN ds >= '20191231' AND ds <= '20200112' THEN quantity 
                        END
                    ) AS total_a2c_unit
                    ,SUM(CASE WHEN ds = '20191231' THEN quantity END) AS a2c_unit_1231
                    ,SUM(CASE WHEN ds = '20200101' THEN quantity END) AS a2c_unit_0101
                    ,SUM(CASE WHEN ds = '20200102' THEN quantity END) AS a2c_unit_0102
                    ,SUM(CASE WHEN ds = '20200103' THEN quantity END) AS a2c_unit_0103
                    ,SUM(CASE WHEN ds = '20200104' THEN quantity END) AS a2c_unit_0104
                    ,SUM(CASE WHEN ds = '20200105' THEN quantity END) AS a2c_unit_0105
                    ,SUM(CASE WHEN ds = '20200106' THEN quantity END) AS a2c_unit_0106
                    ,SUM(CASE WHEN ds = '20200107' THEN quantity END) AS a2c_unit_0107
                    ,SUM(CASE WHEN ds = '20200108' THEN quantity END) AS a2c_unit_0108
                    ,SUM(CASE WHEN ds = '20200109' THEN quantity END) AS a2c_unit_0109
                    ,SUM(CASE WHEN ds = '20200110' THEN quantity END) AS a2c_unit_0110
                    ,SUM(CASE WHEN ds = '20200111' THEN quantity END) AS a2c_unit_0111
                    ,SUM(CASE WHEN ds = '20200112' THEN quantity END) AS a2c_unit_0112
            FROM    lazada_cdm.dwd_lzd_trd_cart_ent_di_vn
            WHERE   venture = 'VN'
            AND     ds >= '20191231'
            GROUP BY product_id
        ) t1
;
----------------------------------
--gmv_l30d/unit_sold_b4_meeting (fixed)--
----------------------------------
DROP TABLE IF EXISTS item_gmv_unit_sold_l30d_bao_tet ;
 
CREATE TABLE item_gmv_unit_sold_l30d_bao_tet AS
SELECT  t1.product_id
        ,gmv_l30d_b4_teasing
        ,unit_sold_b4_teasing
        ,gmv_0105
        ,gmv_0107
FROM    (
            SELECT  product_id
                    ,AVG(
                        CASE    WHEN date_ NOT IN ('20191130','20191212', '20191213', '20191214') AND date_ >= '20191130' AND date_<= '20191230' THEN gmv 
                        END
                    ) AS gmv_l30d_b4_teasing
                    ,AVG(
                        CASE    WHEN date_ NOT IN ('20191130','20191212', '20191213', '20191214') AND date_ >= '20191130' AND date_<= '20191230' THEN cnt_unit_sold 
                        END
                    ) AS unit_sold_b4_teasing
                    ,SUM(
                        CASE    WHEN date_= '20200105' THEN gmv
                        END
                    ) AS gmv_0105
                    ,SUM(
                        CASE    WHEN date_= '20200107' THEN gmv
                        END
                    ) AS gmv_0107
            FROM    (
                        SELECT  product_id
                                ,TO_CHAR(fulfillment_create_date, 'yyyymmdd') AS date_
                                ,SUM(actual_gmv*is_fulfilled*is_revenue*exchange_rate_current) AS gmv
                                ,COUNT(sales_order_item_id) AS cnt_unit_sold
                        FROM    lazada_cdm.dwd_lzd_trd_core_df_vn
                        WHERE   1 = 1
                        AND     is_fulfilled = 1
                        AND     is_revenue = 1
                        AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                        AND     venture = 'VN'
                        AND     TO_CHAR(fulfillment_create_date, 'yyyymmdd') >= '20191130'
                        GROUP BY TO_CHAR(fulfillment_create_date, 'yyyymmdd')
                                 ,product_id
                    ) st1
            GROUP BY product_id
        ) t1
;
----------------------------------
--a2c_gmv--
----------------------------------
DROP TABLE IF EXISTS item_a2c_gmv_bao_tet ;
 
CREATE TABLE item_a2c_gmv_bao_tet AS
SELECT  t5.product_id
        ,total_a2c_gmv
FROM    (
            SELECT  t3.product_id
                    ,total_a2c_unit*min_campaign_price*COALESCE(
                        item_sold_last_mega/nullif(qty_a2c_last_mega,0)
                        ,1
                    ) / 23200 AS total_a2c_gmv
            FROM    (
                        SELECT  t1.product_id
                                ,CASE    WHEN total_a2c_unit IS NULL THEN NULL
                                         WHEN total_a2c_unit IS NOT NULL AND total_a2c_unit < stock THEN total_a2c_unit 
                                         ELSE stock 
                                 END AS total_a2c_unit
                        FROM    (
                                    SELECT  *
                                    FROM    item_a2c_unit_bao_tet
                                ) t1
                        LEFT JOIN (
                                      SELECT  product_id
                                              ,SUM(stock_available) AS stock
                                      FROM    lazada_cdm.dim_lzd_prd_sku_core_vn
                                      WHERE   venture = 'VN'
                                      AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                                      GROUP BY product_id
                                  ) t2
                        ON      t1.product_id = t2.product_id
                    ) t3
            LEFT JOIN (
                        --   SELECT  product_id
                        --           ,min_campaign_price
                        --   FROM    item_price
                            SELECT  tb.product_id
                                    ,MIN(COALESCE(promo.price, campaign_price)) AS min_campaign_price
                            FROM    (
                                        SELECT  child_campaign_id
                                                ,product_id
                                                ,MIN(campaign_promotion_price) AS campaign_price
                                        FROM    lazada_cdm.dim_lzd_pro_treasurebowl_sku_vn
                                        WHERE   1 = 1
                                        AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                                        AND     master_campaign_id = '13534'
                                        AND     campaign_type NOT IN ('slash_it','group_buy','crazy_deal', 'flash_sale', 'brand_mega_offer')
                                        AND     campaign_sku_status = 2
                                        GROUP BY child_campaign_id
                                                 ,product_id
                                    ) tb
                            LEFT JOIN (
                                          SELECT  campaign_id
                                                  ,product_id
                                                  ,MIN(price) AS price
                                          FROM    lazada_cdm.dim_lzd_pro_promprice_vn
                                          WHERE   1 = 1
                                          AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                                          GROUP BY campaign_id
                                                   ,product_id
                                      ) promo
                            ON      tb.child_campaign_id = promo.campaign_id
                            AND     tb.product_id = promo.product_id
                            GROUP BY tb.product_id
                      ) t4
            ON      t3.product_id = t4.product_id LEFT
            JOIN    (
                        SELECT  t7.product_id
                                ,SUM(qty_a2c_last_mega) AS qty_a2c_last_mega
                                ,SUM(item_sold_last_mega) AS item_sold_last_mega
                        FROM    (
                                    SELECT  product_id
                                            ,COUNT(DISTINCT sales_order_item_id) AS item_sold_last_mega
                                    FROM    lazada_cdm.dwd_lzd_trd_core_df_vn
                                    WHERE   1 = 1
                                    AND     TO_CHAR(fulfillment_create_date, 'yyyymmdd') = '20191212'
                                    AND     is_fulfilled = 1
                                    AND     is_revenue = 1
                                    AND     venture = 'VN'
                                    AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                                    GROUP BY product_id
                                ) t7
                        LEFT JOIN (
                                      SELECT  product_id
                                              ,SUM(quantity) AS qty_a2c_last_mega
                                      FROM    lazada_cdm.dwd_lzd_trd_cart_ent_di_vn cart
                                      WHERE   1 = 1
                                      AND     add_date >= TO_DATE('20191212','yyyymmdd')
                                      AND     ds >= '20191201'
                                      AND     ds <= '20191212'
                                      -- AND     hh = 23
                                      AND     is_add = 1
                                      AND     is_delete = 0
                                      GROUP BY product_id
                                  ) t8
                        ON      t7.product_id = t8.product_id
                        GROUP BY t7.product_id
                    ) t9
            ON      t3.product_id = t9.product_id
        ) t5
;
 
----------------------------------
--Traffic--
----------------------------------
DROP TABLE IF EXISTS item_traffic_bao_tet ;
 
CREATE TABLE item_traffic_bao_tet AS
SELECT  a2c.product_id
        ,total_pdp_pv
        ,pdp_pv_l30d
        ,pdp_pv_1231
        ,pdp_pv_0101
        ,pdp_pv_0102
        ,pdp_pv_0103
        ,pdp_pv_0104
        ,pdp_pv_0105
        ,pdp_pv_0106
        ,pdp_pv_0107
        ,pdp_pv_0108
        ,pdp_pv_0109
        ,pdp_pv_0110
        ,pdp_pv_0111
        ,pdp_pv_0112
        ,total_pdp_uv
        ,pdp_uv_1231
        ,pdp_uv_0101
        ,pdp_uv_0102
        ,pdp_uv_0103
        ,pdp_uv_0104
        ,pdp_uv_0105
        ,pdp_uv_0106
        ,pdp_uv_0107
        ,pdp_uv_0108
        ,pdp_uv_0109
        ,pdp_uv_0110
        ,pdp_uv_0111
        ,pdp_uv_0112
        ,total_a2c_uv
        ,a2c_uv_1231
        ,a2c_uv_0101
        ,a2c_uv_0102
        ,a2c_uv_0103
        ,a2c_uv_0104
        ,a2c_uv_0105
        ,a2c_uv_0106
        ,a2c_uv_0107
        ,a2c_uv_0108
        ,a2c_uv_0109
        ,a2c_uv_0110
        ,a2c_uv_0111
        ,a2c_uv_0112
        ,a2c_uv_1212 / nullif(pdp_uv_1212,0) AS cr_a2c_1212
        ,a2c_uv_1231 / nullif(pdp_uv_1231,0) AS cr_a2c_1231
        ,a2c_uv_0101 / nullif(pdp_uv_0101,0) AS cr_a2c_0101
        ,a2c_uv_0102 / nullif(pdp_uv_0102,0) AS cr_a2c_0102
        ,a2c_uv_0103 / nullif(pdp_uv_0103,0) AS cr_a2c_0103
        ,a2c_uv_0104 / nullif(pdp_uv_0104,0) AS cr_a2c_0104
        ,a2c_uv_0105 / nullif(pdp_uv_0105,0) AS cr_a2c_0105
        ,a2c_uv_0106 / nullif(pdp_uv_0106,0) AS cr_a2c_0106
        ,a2c_uv_0107 / nullif(pdp_uv_0107,0) AS cr_a2c_0107
        ,a2c_uv_0108 / nullif(pdp_uv_0108,0) AS cr_a2c_0108
        ,a2c_uv_0109 / nullif(pdp_uv_0109,0) AS cr_a2c_0109
        -- ,a2c_uv_l7d / nullif(pdp_uv_l7d,0) AS cr_a2c_l7d
        ,a2c_uv_0110 / nullif(pdp_uv_0110,0) AS cr_a2c_0110
        ,a2c_uv_0111 / nullif(pdp_uv_0111,0) AS cr_a2c_0111
        ,a2c_uv_0112 / nullif(pdp_uv_0112,0) AS cr_a2c_0112
FROM    (
            SELECT  product_id
                    ,COUNT(DISTINCT CASE WHEN ds = '20191212' THEN utdid END) AS a2c_uv_1212
                    ,COUNT(DISTINCT CASE WHEN ds = '20191231' THEN utdid END) AS a2c_uv_1231
                    ,COUNT(DISTINCT CASE WHEN ds = '20200101' THEN utdid END) AS a2c_uv_0101
                    ,COUNT(DISTINCT CASE WHEN ds = '20200102' THEN utdid END) AS a2c_uv_0102
                    ,COUNT(DISTINCT CASE WHEN ds = '20200103' THEN utdid END) AS a2c_uv_0103
                    ,COUNT(DISTINCT CASE WHEN ds = '20200104' THEN utdid END) AS a2c_uv_0104
                    ,COUNT(DISTINCT CASE WHEN ds = '20200105' THEN utdid END) AS a2c_uv_0105
                    ,COUNT(DISTINCT CASE WHEN ds = '20200106' THEN utdid END) AS a2c_uv_0106
                    ,COUNT(DISTINCT CASE WHEN ds = '20200107' THEN utdid END) AS a2c_uv_0107
                    ,COUNT(DISTINCT CASE WHEN ds = '20200108' THEN utdid END) AS a2c_uv_0108
                    ,COUNT(DISTINCT CASE WHEN ds = '20200109' THEN utdid END) AS a2c_uv_0109
                    ,COUNT(DISTINCT CASE WHEN ds = '20200110' THEN utdid END) AS a2c_uv_0110
                    ,COUNT(DISTINCT CASE WHEN ds = '20200111' THEN utdid END) AS a2c_uv_0111
                    ,COUNT(DISTINCT CASE WHEN ds = '20200112' THEN utdid END) AS a2c_uv_0112
                    ,COUNT(
                        DISTINCT CASE    WHEN ds >= '20191231' AND ds <= '20200112' THEN utdid 
                                 END
                    ) AS total_a2c_uv
                    ,COUNT(
                        DISTINCT CASE    WHEN ds >= '20191223' AND ds <= '20191229' THEN utdid 
                                 END
                    ) AS a2c_uv_l7d
            FROM    lazada_cdm.dwd_lzd_trd_cart_ent_di_vn cart
            WHERE   platform IN ('Wap_Android', 'Native_Android', 'Native_iOS', 'Wap_Unknown', 'Wap_iOS')
            AND     (ds = '20191212' OR ds >= '20191231')
            GROUP BY product_id
        ) a2c
LEFT JOIN (
              --- get traffic
              SELECT  product_id
                      ,COUNT(DISTINCT CASE WHEN ds = '20191212' THEN visitor_id END) AS pdp_uv_1212
                      ,COUNT(DISTINCT CASE WHEN ds = '20191231' THEN visitor_id END) AS pdp_uv_1231
                      ,COUNT(DISTINCT CASE WHEN ds = '20200101' THEN visitor_id END) AS pdp_uv_0101
                      ,COUNT(DISTINCT CASE WHEN ds = '20200102' THEN visitor_id END) AS pdp_uv_0102
                      ,COUNT(DISTINCT CASE WHEN ds = '20200103' THEN visitor_id END) AS pdp_uv_0103
                      ,COUNT(DISTINCT CASE WHEN ds = '20200104' THEN visitor_id END) AS pdp_uv_0104
                      ,COUNT(DISTINCT CASE WHEN ds = '20200105' THEN visitor_id END) AS pdp_uv_0105
                      ,COUNT(DISTINCT CASE WHEN ds = '20200106' THEN visitor_id END) AS pdp_uv_0106
                      ,COUNT(DISTINCT CASE WHEN ds = '20200107' THEN visitor_id END) AS pdp_uv_0107
                      ,COUNT(DISTINCT CASE WHEN ds = '20200108' THEN visitor_id END) AS pdp_uv_0108
                      ,COUNT(DISTINCT CASE WHEN ds = '20200109' THEN visitor_id END) AS pdp_uv_0109
                      ,COUNT(DISTINCT CASE WHEN ds = '20200110' THEN visitor_id END) AS pdp_uv_0110
                      ,COUNT(DISTINCT CASE WHEN ds = '20200111' THEN visitor_id END) AS pdp_uv_0111
                      ,COUNT(DISTINCT CASE WHEN ds = '20200112' THEN visitor_id END) AS pdp_uv_0112
                      ,COUNT(
                          DISTINCT CASE    WHEN ds >= '20191205' AND ds <= '20191214' THEN visitor_id 
                                   END
                      ) AS total_pdp_uv
                      ,COUNT(
                        DISTINCT CASE    WHEN ds >= '20191223' AND ds <= '20191229' THEN visitor_id
                                 END
                    ) AS pdp_uv_l7d
              FROM    lazada_cdm.dws_lzd_log_sku_visitor_1d_vn
              WHERE   1 = 1
              AND     venture = "VN"
              AND     (ds = '20191111' OR ds >= '20191201')
              AND     platform IN ('app', 'msite')
              GROUP BY product_id
          ) ipv
ON      a2c.product_id = ipv.product_id LEFT
JOIN    (
            SELECT  product_id
                    ,SUM(total_ipv) AS total_pdp_pv
                    ,SUM(ipv_l30d) AS pdp_pv_l30d
                    ,SUM(ipv_1231) AS pdp_pv_1231
                    ,SUM(ipv_0101) AS pdp_pv_0101
                    ,SUM(ipv_0102) AS pdp_pv_0102
                    ,SUM(ipv_0103) AS pdp_pv_0103
                    ,SUM(ipv_0104) AS pdp_pv_0104
                    ,SUM(ipv_0105) AS pdp_pv_0105
                    ,SUM(ipv_0106) AS pdp_pv_0106
                    ,SUM(ipv_0107) AS pdp_pv_0107
                    ,SUM(ipv_0108) AS pdp_pv_0108
                    ,SUM(ipv_0109) AS pdp_pv_0109
                    ,SUM(ipv_0110) AS pdp_pv_0110
                    ,SUM(ipv_0111) AS pdp_pv_0111
                    ,SUM(ipv_0112) AS pdp_pv_0112
            FROM    (
                        SELECT  sku
                                ,SUM(CASE WHEN ds = '20191231' THEN ipv_1d_001 END) AS ipv_1231
                                ,SUM(CASE WHEN ds = '20200101' THEN ipv_1d_001 END) AS ipv_0101
                                ,SUM(CASE WHEN ds = '20200102' THEN ipv_1d_001 END) AS ipv_0102
                                ,SUM(CASE WHEN ds = '20200103' THEN ipv_1d_001 END) AS ipv_0103
                                ,SUM(CASE WHEN ds = '20200104' THEN ipv_1d_001 END) AS ipv_0104
                                ,SUM(CASE WHEN ds = '20200105' THEN ipv_1d_001 END) AS ipv_0105
                                ,SUM(CASE WHEN ds = '20200106' THEN ipv_1d_001 END) AS ipv_0106
                                ,SUM(CASE WHEN ds = '20200107' THEN ipv_1d_001 END) AS ipv_0107
                                ,SUM(CASE WHEN ds = '20200108' THEN ipv_1d_001 END) AS ipv_0108
                                ,SUM(CASE WHEN ds = '20200109' THEN ipv_1d_001 END) AS ipv_0109
                                ,SUM(CASE WHEN ds = '20200110' THEN ipv_1d_001 END) AS ipv_0110
                                ,SUM(CASE WHEN ds = '20200111' THEN ipv_1d_001 END) AS ipv_0111
                                ,SUM(CASE WHEN ds = '20200112' THEN ipv_1d_001 END) AS ipv_0112
                                ,SUM(
                                    CASE    WHEN ds >= '20191231' AND ds <= '20200112' THEN ipv_1d_001 
                                    END
                                ) AS total_ipv
                                ,AVG(
                                    CASE    WHEN ds NOT IN ('20191130','20191212', '20191213', '20191214') AND ds >= '20191130' AND ds <= '20191230' THEN ipv_1d_001 
                                    END
                                ) AS ipv_l30d
                        FROM    lazada_cdm.dws_lzd_log_sku_pv_uv_all_1d_vn
                        WHERE   venture = 'VN'
                        AND     platform IN ('app')
                        AND     ds >= '20191130'
                        GROUP BY sku
                    ) st1
            LEFT JOIN (
                          SELECT  lazada_sku
                                  ,product_id
                          FROM    lazada_cdm.dim_lzd_prd_sku_core_vn
                      ) st2
            ON      st1.sku = st2.lazada_sku
            GROUP BY product_id
        ) vst
ON      a2c.product_id = vst.product_id
;
----------------------------------
--Stock--
----------------------------------
 
DROP TABLE IF EXISTS item_stock_bao_tet ;
 
CREATE TABLE item_stock_bao_tet AS
SELECT  t1.product_id
        ,stock
        ,CASE    WHEN stock = 0 THEN 1 
                 ELSE 0 
         END AS is_OOS
        ,CASE    WHEN stock >= (cnt_unit_sold_last_mega) AND stock >= total_a2c_unit THEN 1 
                 ELSE 0 
         END AS is_stock_enough
FROM    (
            SELECT  product_id
                    ,SUM(stock_available) AS stock
            FROM    lazada_cdm.dim_lzd_prd_sku_core_vn
            WHERE   venture = 'VN'
            GROUP BY product_id
        ) t1
LEFT JOIN (
              SELECT  product_id
                      ,COUNT(sales_order_item_id) AS cnt_unit_sold_last_mega
              FROM    lazada_cdm.dwd_lzd_trd_core_df_vn
              WHERE   TO_CHAR(fulfillment_create_date, 'yyyymmdd') = '20191212'
              GROUP BY product_id
          ) t2
ON      t1.product_id = t2.product_id LEFT
JOIN    (
            SELECT  product_id
                    ,total_a2c_unit
            FROM    item_a2c_unit_bao_tet
        ) t3
ON      t2.product_id = t3.product_id
;
----------------------------------
--Estimated gmv--
----------------------------------
 
DROP TABLE IF EXISTS item_estimated_gmv_bao_tet ;
 
CREATE TABLE item_estimated_gmv_bao_tet AS
SELECT  t4.product_id
        ,total_a2c_gmv * shopping_cart_cr_last_mega AS estimated_gmv_total
FROM    (
            SELECT  t1.product_id
                    ,total_a2c_gmv
                    ,cnt_unit_sold_last_mega / nullif(a2c_quantity_last_mega,0) AS shopping_cart_cr_last_mega
            FROM    (
                        SELECT  product_id
                                ,SUM(quantity) AS a2c_quantity_last_mega
                        FROM    lazada_cdm.dwd_lzd_trd_cart_ent_di_vn
                        WHERE   venture = 'VN'
                        AND     ds = '20191212'
                        GROUP BY product_id
                    ) t1
            LEFT JOIN (
                          SELECT  product_id
                                  ,COUNT(sales_order_item_id) AS cnt_unit_sold_last_mega
                          FROM    lazada_cdm.dwd_lzd_trd_core_df_vn
                          WHERE   venture = 'VN'
                          AND     is_fulfilled = 1
                          AND     is_revenue = 1
                          AND     TO_CHAR(fulfillment_create_date, 'yyyymmdd') = '20191212'
                          AND     1 = 1
                          GROUP BY product_id
                      ) t2
            ON      t1.product_id = t2.product_id LEFT
            JOIN    (
                        SELECT  product_id
                                ,total_a2c_gmv
                        FROM    item_a2c_gmv_bao_tet
                    ) t3
            ON      t3.product_id = t1.product_id
        ) t4
;
 
----------------------------------
--Price--
----------------------------------
DROP TABLE IF EXISTS item_price_tet ;
 
CREATE TABLE item_price_tet AS
SELECT  t2.product_id
        ,competitor_price
        ,min_campaign_price
        ,min_current_price
        -- ,avg_price_l60d
        ,1 - (min_campaign_price/nullif(min_current_price,0)) AS discount_rate
        -- ,min_price_l30d_b4_teasing
        -- ,CASE    WHEN min_campaign_price_1111 > current_price AND min_campaign_price_1111 IS NOT NULL AND current_price IS NOT NULL THEN 1 
        --          ELSE 0 
        --  END AS is_campaign_price_higher_current_price
        ,CASE    WHEN min_campaign_price > competitor_price AND min_campaign_price IS NOT NULL AND competitor_price IS NOT NULL THEN 1 
                 ELSE 0 
         END AS is_campaign_price_higher_competitor_price
FROM    (
            --             SELECT  product_id
            --                     ,non_fs_min_price_l30d AS min_price_l30d_b4_teasing
            --             FROM    lazada_cdm.dim_lzd_prd_sku_ext_vn
            --             WHERE   ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
            --         ) t1
            -- ON      t0.product_id = t1.product_id LEFT
            -- JOIN    (
            SELECT  product_id
                    ,MIN(competitor_price) AS competitor_price
                    ,MIN(current_price) AS min_current_price
            FROM    lazada_cdm.dim_lzd_prd_sku_core_vn
            WHERE   venture = 'VN'
            AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
            GROUP BY product_id
        ) t2
LEFT JOIN (
              SELECT  tb.product_id
                      ,MIN(COALESCE(promo.price, campaign_price)) AS min_campaign_price
              FROM    (
                          SELECT  child_campaign_id
                                  ,product_id
                                  ,MIN(campaign_promotion_price) AS campaign_price
                          FROM    lazada_cdm.dim_lzd_pro_treasurebowl_sku_vn
                          WHERE   1 = 1
                          AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                          AND     master_campaign_id = '13534'
                          AND     campaign_type NOT IN ('slash_it','group_buy','crazy_deal', 'flash_sale', 'brand_mega_offer')
                          AND     campaign_sku_status = 2
                          GROUP BY child_campaign_id
                                   ,child_campaign_name
                                   ,product_id
                      ) tb
              LEFT JOIN (
                            SELECT  campaign_id
                                    ,product_id
                                    ,MIN(price) AS price
                            FROM    lazada_cdm.dim_lzd_pro_promprice_vn
                            WHERE   1 = 1
                            AND     ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
                            GROUP BY campaign_id
                                     ,product_id
                        ) promo
              ON      tb.child_campaign_id = promo.campaign_id
              AND     tb.product_id = promo.product_id
              GROUP BY tb.product_id
          ) t3
ON      t2.product_id = t3.product_id 
-- LEFT
-- JOIN    (
--             SELECT  product_id
--                     ,AVG(avg_price_l60d) AS avg_price_l60d
--             FROM    lazada_cdm.dim_lzd_prd_sku_ext_vn
--             WHERE   ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
--             GROUP BY product_id
--         ) t4
-- ON      t2.product_id = t4.product_id
;



----------------------------------
-- check_duplication--
----------------------------------
-- SELECT COUNT(product_id), count(DISTINCT product_id) 
-- FROM 2019_11111_top200_info
-- ;
 
----------------------------------
--data extract--
----------------------------------
DROP TABLE IF EXISTS data_consolidation_bao_tet ;
 
CREATE TABLE data_consolidation_bao_tet AS
SELECT  t1.product_id    --info
        ,business_type
        ,business_type_level2
        ,cluster_name
        ,product_name
        ,seller_name
        ,seller_id
        ,seller_short_code
        ,commercial_category_name
        ,regional_category1_name
        ,regional_category2_name
        ,regional_category3_name
        --a2c_unit--
        ,total_a2c_unit
        ,a2c_unit_1231
        ,a2c_unit_0101
        ,a2c_unit_0102
        ,a2c_unit_0103
        ,a2c_unit_0104
        ,a2c_unit_0105
        ,a2c_unit_0106
        ,a2c_unit_0107
        ,a2c_unit_0108
        ,a2c_unit_0109
        ,a2c_unit_0110
        ,a2c_unit_0111
        ,a2c_unit_0112
        --b4_teasing--
        ,gmv_l30d_b4_teasing
        ,unit_sold_b4_teasing
        ,gmv_0105
        ,gmv_0107
        --a2c_gmv--
        ,total_a2c_gmv
        --traffic--
        ,total_pdp_pv
        ,pdp_pv_l30d
        ,pdp_pv_1231
        ,pdp_pv_0101
        ,pdp_pv_0102
        ,pdp_pv_0103
        ,pdp_pv_0104
        ,pdp_pv_0105
        ,pdp_pv_0106
        ,pdp_pv_0107
        ,pdp_pv_0108
        ,pdp_pv_0109
        ,pdp_pv_0110
        ,pdp_pv_0111
        ,pdp_pv_0112
        ,total_pdp_uv
        ,pdp_uv_1231
        ,pdp_uv_0101
        ,pdp_uv_0102
        ,pdp_uv_0103
        ,pdp_uv_0104
        ,pdp_uv_0105
        ,pdp_uv_0106
        ,pdp_uv_0107
        ,pdp_uv_0108
        ,pdp_uv_0109
        ,pdp_uv_0110
        ,pdp_uv_0111
        ,pdp_uv_0112
        ,total_a2c_uv
        ,a2c_uv_1231
        ,a2c_uv_0101
        ,a2c_uv_0102
        ,a2c_uv_0103
        ,a2c_uv_0104
        ,a2c_uv_0105
        ,a2c_uv_0106
        ,a2c_uv_0107
        ,a2c_uv_0108
        ,a2c_uv_0109
        ,a2c_uv_0110
        ,a2c_uv_0111
        ,a2c_uv_0112
        ,cr_a2c_1212
        ,cr_a2c_1231
        ,cr_a2c_0101
        ,cr_a2c_0102
        ,cr_a2c_0103
        ,cr_a2c_0104
        ,cr_a2c_0105
        ,cr_a2c_0106
        ,cr_a2c_0107
        ,cr_a2c_0108
        ,cr_a2c_0109
        ,cr_a2c_0110
        ,cr_a2c_0111
        ,cr_a2c_0112
        --stock--
        ,stock
        ,is_OOS
        ,is_stock_enough
        --estimated gmv--
        ,estimated_gmv_total
        --price--
        ,competitor_price
        ,min_campaign_price
        ,min_current_price
        ,discount_rate
        ,is_campaign_price_higher_competitor_price
FROM    item_info_bao_tet t1
LEFT JOIN item_a2c_unit_bao_tet t2
ON      t1.product_id = t2.product_id LEFT
JOIN    item_gmv_unit_sold_l30d_bao_tet t3
ON      t1.product_id = t3.product_id
LEFT JOIN item_traffic_bao_tet t4
ON      t1.product_id = t4.product_id LEFT
JOIN    item_stock_bao_tet t5
ON      t1.product_id = t5.product_id
LEFT JOIN item_estimated_gmv_bao_tet t6
ON      t1.product_id = t6.product_id LEFT
JOIN    item_price_tet t7
ON      t1.product_id = t7.product_id
LEFT JOIN item_a2c_gmv_bao_tet t8
ON      t1.product_id = t8.product_id
;
 
DROP TABLE IF EXISTS hotdeal_jan7 ;
 
CREATE TABLE hotdeal_jan7 AS
SELECT  t1.product_id
        ,business_type
        ,business_type_level2
        ,cluster_name
        ,product_name
        ,seller_name
        ,seller_id
        ,seller_short_code
        ,commercial_category_name
        ,regional_category1_name
        ,regional_category2_name
        ,regional_category3_name
        --a2c_unit--
        ,total_a2c_unit
        ,a2c_unit_1231
        ,a2c_unit_0101
        ,a2c_unit_0102
        ,a2c_unit_0103
        ,a2c_unit_0104
        ,a2c_unit_0105
        ,a2c_unit_0106
        ,a2c_unit_0107
        ,a2c_unit_0108
        ,a2c_unit_0109
        ,a2c_unit_0110
        ,a2c_unit_0111
        ,a2c_unit_0112
        --b4_teasing--
        ,gmv_l30d_b4_teasing
        ,unit_sold_b4_teasing
        ,gmv_0105
        ,gmv_0107
        --a2c_gmv--
        ,total_a2c_gmv
        --traffic--
        ,total_pdp_pv
        ,pdp_pv_l30d
        ,pdp_pv_1231
        ,pdp_pv_0101
        ,pdp_pv_0102
        ,pdp_pv_0103
        ,pdp_pv_0104
        ,pdp_pv_0105
        ,pdp_pv_0106
        ,pdp_pv_0107
        ,pdp_pv_0108
        ,pdp_pv_0109
        ,pdp_pv_0110
        ,pdp_pv_0111
        ,pdp_pv_0112
        ,total_pdp_uv
        ,pdp_uv_1231
        ,pdp_uv_0101
        ,pdp_uv_0102
        ,pdp_uv_0103
        ,pdp_uv_0104
        ,pdp_uv_0105
        ,pdp_uv_0106
        ,pdp_uv_0107
        ,pdp_uv_0108
        ,pdp_uv_0109
        ,pdp_uv_0110
        ,pdp_uv_0111
        ,pdp_uv_0112
        ,total_a2c_uv
        ,a2c_uv_1231
        ,a2c_uv_0101
        ,a2c_uv_0102
        ,a2c_uv_0103
        ,a2c_uv_0104
        ,a2c_uv_0105
        ,a2c_uv_0106
        ,a2c_uv_0107
        ,a2c_uv_0108
        ,a2c_uv_0109
        ,a2c_uv_0110
        ,a2c_uv_0111
        ,a2c_uv_0112
        ,cr_a2c_1212
        ,cr_a2c_1231
        ,cr_a2c_0101
        ,cr_a2c_0102
        ,cr_a2c_0103
        ,cr_a2c_0104
        ,cr_a2c_0105
        ,cr_a2c_0106
        ,cr_a2c_0107
        ,cr_a2c_0108
        ,cr_a2c_0109
        ,cr_a2c_0110
        ,cr_a2c_0111
        ,cr_a2c_0112
        --stock--
        ,stock
        ,is_OOS
        ,is_stock_enough
        --estimated gmv--
        ,estimated_gmv_total
        --price--
        ,competitor_price
        ,min_campaign_price
        ,min_current_price
        ,discount_rate
        ,is_campaign_price_higher_competitor_price
FROM    (
            SELECT  product_id
            FROM    hotdeal_200107
        ) t1
LEFT JOIN (
              SELECT  *
              FROM    data_consolidation_bao_tet
          ) t2
ON      t1.product_id = t2.product_id
;
 
------------------------------
SELECT  *
FROM    (
            SELECT  *
                    ,ROW_NUMBER() OVER(ORDER BY bu_name) AS row_num
            FROM    lazglobal_deboost
        ) t1
WHERE   row_num > 4000
AND     row_num <= 6000
;
 
DROP TABLE IF EXISTS a;
CREATE TABLE a AS
 
SELECT LEFT(date,6) AS month_ 
        ,COUNT(DISTINCT user_id) AS MAU
FROM table_
GROUP BY LEFT(date,6)
 
;
SELECT t3.*
FROM 
(SELECT DATEDIFF(TO_DATE(t2.month_, 'yyyymm'), TO_DATE(t1.month_,'yyyymm'), 'mm') AS month_gap
        ,t2.month_ AS month_
        ,t2.month_/NULLIF(t1.month_, 0) -1 AS MoM
(SELECT month_, MAU
FROM a) t1
LEFT JOIN
(SELECT month_, MAU
FROM a) t2 
ON t1.month_ = t2.month_) t3
WHERE month_gap IS NOT NULL
AND month_gap = 1
;
 
DROP TABLE IF b;
CREATE TABLE b AS 
SELECT TO_DATE(LEFT(date,7),'yyyymm') AS month_
        ,user_id
FROM table_
;
SELECT month_
        ,COUNT(DISTINCT user_id) AS retent_user 
FROM
(SELECT DATEDIFF(t2.month_,t1.month_,'mm') AS month_diff
        ,t1.month_
        ,t1.user_id
FROM 
(SELECT MIN(month_) AS month_
        ,user_id
FROM b) t1
INNER JOIN
(SELECT month_
        ,user_id
FROM b) t2
ON t1.user_id = t2.user_id) t3
WHERE month_diff > 0
;

