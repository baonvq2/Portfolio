CREATE OR REPLACE TABLE
  `batdongsan-datalake-v0.baonvq.bao_health_mtd` AS
SELECT t0.*,
        'Week' AS data_type
FROM 
(SELECT
  t1.date_,
  t1.VipType,
  t3.Name,
  t1.CateID,
  t4.CateName,
  t1.CreateByUser,
  t5.CustomerType,
  t5.UserType,
  COUNT(DISTINCT CASE WHEN t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN t1.ProductID END) AS unique_listing,
  COUNT(DISTINCT
    CASE
      WHEN t1.ListingType = 'New' AND t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN t1.ProductID
  END
    ) AS new_unique_listing,
  COUNT(DISTINCT
    CASE
      WHEN t1.ListingType = 'Renew' AND t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN t1.ProductID
  END
    ) AS new_existing_listing,
  COUNT(DISTINCT
    CASE
      WHEN t1.VipType = 5 AND t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN t1.ProductID
  END
    ) AS normal_listing,
  COUNT(DISTINCT
    CASE
      WHEN t1.VipType <> 5 AND t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN t1.ProductID
  END
    ) AS VIP_listing,
  SUM(CASE WHEN t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN impression END) AS impression,
  SUM(CASE WHEN t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN pageview END) AS pageview,
  SUM(CASE WHEN t1.DateId >= CAST(FORMAT_DATE('%Y%m%d',DATE_TRUNC(t1.BeginTime, MONTH)) AS int64) THEN leads END) AS leads
FROM (
  SELECT
    DateId,
    VipType,
    CateID,
    CreateByUser,
    ListingType,
    ProductID,
    FORMAT_DATE('%Y%m%d', BeginTime) AS date_,
    BeginTime
  FROM
    `batdongsan-datalake-v0.dwh.fact_listing_v2`,
    UNNEST(trxs) AS trxs,
    UNNEST(listingDays) AS listingDays) t1
LEFT JOIN (
  SELECT
    CAST(CAST(FORMAT_DATE('%Y%m%d', date) AS STRING) AS int64) AS date1,
    product_id,
    SUM(total_impression) AS impression,
    SUM(total_pageview) AS pageview,
    SUM(total_lead) AS leads
  FROM
    `batdongsan-datalake-v0.dwh.fact_product_tracking`
  GROUP BY
    date1,
    product_id) t2
ON
  (t1.ProductID = t2.product_id
    AND t1.DateId = t2.date1)
LEFT JOIN
  `batdongsan-datalake-v0.dwh.dim_vip_type` t3
ON
  t1.VipType = t3.Id
LEFT JOIN
  `batdongsan-datalake-v0.dwh.dim_categories` t4
ON
  t1.CateID = t4.CateID
LEFT JOIN
  `batdongsan-datalake-v0.dwh.dim_user` t5
ON
  t1.CreateByUser = t5.Id
GROUP BY
  t1.date_,
  t1.VipType,
  t3.Name,
  t1.CateID,
  t4.CateName,
  t1.CreateByUser,
  t5.CustomerType,
  t5.UserType) t0
;
