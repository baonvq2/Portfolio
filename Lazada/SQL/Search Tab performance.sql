CREATE TABLE IF NOT EXISTS Search_tab_performance_summary_daily_Bao1 
(
    venture STRING
    ,filter_name STRING
    ,pv DOUBLE
    ,item_pv DOUBLE
    ,uv DOUBLE
    ,non_result_pv DOUBLE
    ,se_instance DOUBLE
    ,0_se_instance DOUBLE
    ,ipv DOUBLE
    ,ipv_uv DOUBLE
    ,cart_uv DOUBLE
    ,pay_uv DOUBLE
    ,pay_cnt DOUBLE
    ,pay_amt DOUBLE
    ,pay_qty DOUBLE
)
PARTITIONED BY 
(
    ds STRING
)
LIFECYCLE 400
;
 
INSERT OVERWRITE TABLE Search_tab_performance_summary_daily_Bao1 PARTITION(ds)
SELECT  venture
        ,filter_name AS filter_name
        ,MAX(pv) AS pv
        ,MAX(item_pv) AS item_pv
        ,MAX(uv) AS uv
        ,MAX(non_result_pv) AS non_result_pv
        ,MAX(se_instance) AS se_instance
        ,MAX(0_se_instance) AS 0_se_instance
        ,MAX(ipv) AS ipv
        ,MAX(ipv_uv) AS ipv_uv
        ,MAX(cart_uv) AS cart_uv
        ,MAX(pay_uv) AS pay_uv
        ,MAX(pay_cnt) AS pay_cnt
        ,MAX(pay_amt) AS pay_amt
        ,MAX(pay_qty) AS pay_qty
        ,ds
FROM    (
            SELECT  ds
                    --曝光指标--
                    ,venture
                    ,KEYVALUE(TOLOWER(wsurl),'&','=','tab') AS filter_name
                    ,COUNT(1) AS pv
                    ,CAST(0 AS BIGINT) AS item_pv
                    ,COUNT(
                        DISTINCT CASE    WHEN LENGTH(cookie_user_id) > 1 THEN cookie_user_id 
                                         ELSE utdid 
                                 END
                    ) AS uv
                    ,CAST(0 AS BIGINT) AS non_bounce_uv
                    ,COUNT(CASE WHEN found_num = 0 THEN 1 END) AS non_result_pv
                    ,COUNT(DISTINCT CASE WHEN page_seq=1 THEN rn END) AS se_instance
                    ,COUNT(
                        DISTINCT CASE    WHEN page_seq=1 AND found_num = 0 THEN rn 
                                 END
                    ) AS 0_se_instance
                    ,COUNT(DISTINCT COALESCE(se_keyword,''),visitor_id) AS query_cnt
                    ,COUNT(DISTINCT COALESCE(se_keyword,'')) AS unique_query_cnt
                    ,CAST(0 AS BIGINT) AS ipv
                    ,CAST(0 AS BIGINT) AS ipv_uv
                    ,CAST(0 AS BIGINT) AS cart_uv
                    ,CAST(0 AS BIGINT) AS pay_uv
                    ,CAST(0 AS BIGINT) AS pay_cnt
                    ,CAST(0 AS DOUBLE) AS pay_amt
                    ,CAST(0 AS BIGINT) AS pay_qty
            FROM    lazada_cdm.dwd_lzd_log_se_wl_pv_di
            WHERE   ds = '${bizdate}'
            -- WHERE   ds >= TO_CHAR(DATETRUNC(GETDATE(), 'yyyy'), 'yyyymmdd')
            -- WHERE ds >= TO_CHAR(DATEADD(getdate(), - 30, 'day'), 'yyyymmdd')
            AND     TOLOWER(srp_name) NOT IN ('lazadainshopsrp')
            AND     venture IN ('VN')
            AND     lower(search_scenario) IN ('keyword')
            AND     ( (lower(search_scenario) <> 'seller' OR search_scenario IS NOT NULL ))
            GROUP BY ds
                     ,venture
                     ,KEYVALUE(TOLOWER(wsurl),'&','=','tab')
            UNION ALL
            SELECT  ds
                    ,venture
                    ,KEYVALUE(TOLOWER(wsurl),'&','=','tab') AS filter_name
                    ,CAST(0 AS BIGINT) AS pv
                    ,COUNT(1) AS item_pv
                    ,CAST(0 AS BIGINT) AS uv
                    ,CAST(0 AS BIGINT) AS non_bounce_uv
                    ,CAST(0 AS BIGINT) AS non_result_pv
                    ,CAST(0 AS BIGINT) AS se_instance
                    ,CAST(0 AS BIGINT) AS 0_se_instance
                    ,CAST(0 AS BIGINT) AS query_cnt
                    ,CAST(0 AS BIGINT) AS unique_query_cnt
                    ,CAST(0 AS BIGINT) AS ipv
                    ,CAST(0 AS BIGINT) AS ipv_uv
                    ,CAST(0 AS BIGINT) AS cart_uv
                    ,CAST(0 AS BIGINT) AS pay_uv
                    ,CAST(0 AS BIGINT) AS pay_cnt
                    ,CAST(0 AS DOUBLE) AS pay_amt
                    ,CAST(0 AS BIGINT) AS pay_qty
            FROM    lazada_cdm.dwd_lzd_log_se_wl_pv_item_di
            WHERE   ds = '${bizdate}'
            -- WHERE   ds >= TO_CHAR(DATETRUNC(GETDATE(), 'yyyy'), 'yyyymmdd')
            -- WHERE ds >= TO_CHAR(DATEADD(getdate(), - 60, 'day'), 'yyyymmdd')
            AND     TOLOWER(srp_name) NOT IN ('lazadainshopsrp')
            AND     venture IN ('VN')
            AND     lower(search_scenario) IN ('keyword')
            AND     ( (lower(search_scenario) <> 'seller' OR search_scenario IS NOT NULL ))
            GROUP BY ds
                     ,venture
                     ,KEYVALUE(TOLOWER(wsurl),'&','=','tab')
            UNION ALL
            --点击相关指标--
            SELECT  ds
                    ,venture
                    ,KEYVALUE(TOLOWER(wsurl),'&','=','tab') AS filter_name
                    ,CAST(0 AS BIGINT) AS pv
                    ,CAST(0 AS BIGINT) AS item_pv
                    ,CAST(0 AS BIGINT) AS uv
                    ,CAST(0 AS BIGINT) AS non_bounce_uv
                    ,CAST(0 AS BIGINT) AS non_result_pv
                    ,CAST(0 AS BIGINT) AS se_instance
                    ,CAST(0 AS BIGINT) AS 0_se_instance
                    ,CAST(0 AS BIGINT) AS query_cnt
                    ,CAST(0 AS BIGINT) AS unique_query_cnt
                    ,COUNT(1) AS ipv
                    ,COUNT(
                        DISTINCT CASE    WHEN LENGTH(cookie_user_id) > 1 THEN cookie_user_id 
                                         ELSE utdid 
                                 END
                    ) AS ipv_uv
                    ,COUNT(DISTINCT CASE WHEN is_cart = 1 THEN visitor_id END) AS cart_uv
                    ,CAST(0 AS BIGINT) AS pay_uv
                    ,CAST(0 AS BIGINT) AS pay_cnt
                    ,CAST(0 AS DOUBLE) AS pay_amt
                    ,CAST(0 AS BIGINT) AS pay_qty
            FROM    lazada_cdm.dwd_lzd_log_se_wl_ipv_di
            WHERE   ds = '${bizdate}'
            -- WHERE   ds >= TO_CHAR(DATETRUNC(GETDATE(), 'yyyy'), 'yyyymmdd')
            -- WHERE ds >= TO_CHAR(DATEADD(getdate(), - 60, 'day'), 'yyyymmdd')
            AND     TOLOWER(srp_name) NOT IN ('lazadainshopsrp')
            AND     venture IN ('VN')
            AND     lower(search_scenario) IN ('keyword')
            AND     ( (lower(search_scenario) <> 'seller' OR search_scenario IS NOT NULL ))
            GROUP BY ds
                     ,venture
                     ,KEYVALUE(TOLOWER(wsurl),'&','=','tab')
            UNION ALL
            --成交相关指标--
            SELECT  ds
                    ,venture
                    ,KEYVALUE(TOLOWER(wsurl),'&','=','tab') AS filter_name
                    ,CAST(0 AS BIGINT) AS pv
                    ,CAST(0 AS BIGINT) AS item_pv
                    ,CAST(0 AS BIGINT) AS uv
                    ,CAST(0 AS BIGINT) AS non_bounce_uv
                    ,CAST(0 AS BIGINT) AS non_result_pv
                    ,CAST(0 AS BIGINT) AS se_instance
                    ,CAST(0 AS BIGINT) AS 0_se_instance
                    ,CAST(0 AS BIGINT) AS query_cnt
                    ,CAST(0 AS BIGINT) AS unique_query_cnt
                    ,CAST(0 AS BIGINT) AS ipv
                    ,CAST(0 AS BIGINT) AS ipv_uv
                    ,CAST(0 AS BIGINT) AS cart_uv
                    -- ,count(distinct buyer_id) as pay_uv              
                    -- ,count(distinct order_id) as pay_cnt  
                    ,COUNT(
                        DISTINCT CASE    WHEN direct_lead_pay_ord_amt>0 THEN buyer_id 
                                         ELSE NULL 
                                 END
                    ) AS pay_uv
                    ,COUNT(
                        DISTINCT CASE    WHEN direct_lead_pay_ord_amt>0 THEN order_id 
                                         ELSE NULL 
                                 END
                    ) AS pay_cnt
                    ,SUM(direct_lead_pay_ord_amt*exchange_rate) AS pay_amt
                    ,SUM(direct_lead_pay_ord_cnt) AS pay_qty
            FROM    lazada_cdm.dwd_lzd_log_wl_se_lead_pay_di
            WHERE   ds = '${bizdate}'
            -- WHERE   ds >= TO_CHAR(DATETRUNC(GETDATE(), 'yyyy'), 'yyyymmdd')
            -- WHERE ds >= TO_CHAR(DATEADD(getdate(), - 60, 'day'), 'yyyymmdd')
            AND     TOLOWER(srp_name) NOT IN ('lazadainshopsrp')
            AND     venture IN ('VN')
            AND     lower(search_scenario) IN ('keyword')
            AND     ( (lower(search_scenario) <> 'seller' OR search_scenario IS NOT NULL ))
            GROUP BY ds
                     ,venture
                     ,KEYVALUE(TOLOWER(wsurl),'&','=','tab')
        ) t
GROUP BY venture
         ,filter_name, ds
;
 
SELECT * FROM Search_tab_performance_summary_daily_Bao1;
SELECT user_id
        ,month_ 
        ,SUM(CASE WHEN datediff(month_,) THEN revenue END) as rev_3m 
FROM 
(SELECT TO_CHAR(date, yyyymm) as month_   
        ,a.*
FROM a) t1

