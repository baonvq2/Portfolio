WITH
  acp AS (
  SELECT
    phone
  FROM (
    SELECT
      SPLIT(REPLACE(REGEXP_REPLACE(public.decrypt(PhoneNumber,
              'phone'), '\\([^()]*\\)', ''), ',', ';'), ';') AS phones
    FROM
      `batdongsan-datalake-v0.dwh.dim_customer`),
    UNNEST(phones) phone),
    ap as (SELECT
  DISTINCT phone
FROM (
  SELECT
    DISTINCT phone
  FROM (
    SELECT
      SPLIT(REPLACE(REGEXP_REPLACE(public.decrypt(SecondaryNumber,
              'phone'), '\\([^()]*\\)', ''), ',', ';'), ';') AS phones
    FROM
      `batdongsan-datalake-v0.staging_bds.user_mobile_phone`),
    UNNEST(phones) phone
  UNION ALL
  SELECT
    REPLACE(REPLACE(public.decrypt(PrimaryNumber,
          'phone'), '+', ''), '_', '') phone
  FROM
    `staging_bds.user_mobile_phone`
      UNION ALL
  SELECT
    REPLACE(REPLACE(public.decrypt(MobilePhone,
          'phone'), '+', ''), '_', '') phone
  FROM
    `staging_bds.users`)
WHERE
  phone IS NOT NULL
  AND phone != ''),
  ct AS (
  SELECT
    du.id userid,
    public.decrypt(du.phone,
      'phone') phone,
    COUNT(DISTINCT fl.list_id) total_listing,
    COUNT(DISTINCT
    IF
      (dc.type_name = 'Cho thuê'
        AND dc.cate_name!='Căn hộ/Chung cư' and dl.province_name='Tp Hồ Chí Minh',
        fl.list_id,
        NULL) ) total_rent_hcm_non_department,
    MAX(fl.list_date) last_listing,
  FROM
    `competitor_chotot.fact_listing_*` fl
  LEFT JOIN
    `competitor_chotot.dim_user` du
  ON
    fl.account_id =du.id
  LEFT JOIN
    `competitor_chotot.dim_category` dc
  ON
    fl.category_id=dc.id
  left join `competitor_chotot.dim_location` dl on fl.location_id=dl.id
  GROUP BY
    1,
    2)
SELECT
  *
FROM
  ct
LEFT JOIN
  acp
ON
  ct.phone = acp.phone
left join ap on ct.phone=ap.phone
WHERE
  acp.phone IS NULL
  and ap.phone is null
  AND ct.phone IS NOT NULL and total_rent_hcm_non_department > 0
