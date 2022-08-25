WITH chotot_mapping_crawler_211229 AS (SELECT PARSE_TIMESTAMP("%Y%m%d",date_) AS date, date_, list_date, list_id, account_id, project_id, company_ad, owner, price, size, living_size, ifnull(price/size,0) AS unitprice, house_type, category_id, type_name, a.cate_name, bds_cate, b.bds_cate_name, location_id, TRIM(province) AS province, TRIM(district) AS district, TRIM(ward) AS ward, street_id, c.bds_province AS mapped_province, d.bds_district AS mapped_district, e.bds_ward AS mapped_ward
FROM
(SELECT t1.*, t2.cate_name,
    t2.type_id,
    t3.province_id,
    t3.province_name AS province,
    t3.district_id,
    t3.district_name AS district,
    t3.ward_id,
    t3.ward_name AS ward,
FROM
(SELECT *, _TABLE_SUFFIX AS date_ FROM `batdongsan-datalake-v0.competitor_chotot.fact_listing_*` ) t1
LEFT JOIN `batdongsan-datalake-v0.competitor_chotot.dim_category` t2 ON t1.category_id = t2.id
LEFT JOIN `batdongsan-datalake-v0.competitor_chotot.dim_location` t3 ON t1.location_id = t3.id) a
LEFT JOIN `batdongsan-datalake-v0.baonvq.211228_mapping_cate_crawler` b ON a.type_id = b.type_id AND a.cate_name = b.cate_name
LEFT JOIN (SELECT DISTINCT chotot_province, bds_province FROM `batdongsan-datalake-v0.baonvq.province_and_district_map_bds_chotot_alo`) c ON a.province = c.chotot_province
LEFT JOIN (SELECT DISTINCT chotot_province, chotot_district, bds_district FROM `batdongsan-datalake-v0.baonvq.province_and_district_map_bds_chotot_alo`)d ON a.province = d.chotot_province AND a.district = d.chotot_district
LEFT JOIN (SELECT DISTINCT chotot_province, chotot_district, chotot_ward, bds_ward FROM `batdongsan-datalake-v0.baonvq.hcm_hn_district_ward_mapping_bds_chotot_alo`) e ON a.province = e.chotot_province AND a.district = e.chotot_district AND a.ward = e.chotot_ward
WHERE price > 100000000 AND price < 1000000000000)
, t3 AS (SELECT DISTINCT CONCAT(CityName,DistrictName,WardName) AS location, CityCode, DistrictId, WardId   FROM `batdongsan-datalake-v0.dwh.dim_location`)
, t4 AS (SELECT DISTINCT date_,b.bds_cate_name,  c.bds_province AS mapped_province, d.bds_district AS mapped_district, e.bds_ward AS mapped_ward, APPROX_QUANTILES(ifnull(price/size,0),100)[OFFSET(7)] AS percentile7, APPROX_QUANTILES(ifnull(price/size,0),100)[OFFSET(95)] AS percentile99
FROM
(SELECT t1.*, t2.cate_name,
    t2.type_id,
    t3.province_id,
    t3.province_name AS province,
    t3.district_id,
    t3.district_name AS district,
    t3.ward_id,
    t3.ward_name AS ward,
FROM
(SELECT *, _TABLE_SUFFIX AS date_ FROM `batdongsan-datalake-v0.competitor_chotot.fact_listing_*` 
WHERE ((( PARSE_TIMESTAMP("%Y%m%d",_TABLE_SUFFIX)  ) >= ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'Asia/Ho_Chi_Minh'), MONTH, 'Asia/Ho_Chi_Minh'), 'Asia/Ho_Chi_Minh'), INTERVAL -6 MONTH), 'Asia/Ho_Chi_Minh'))) AND ( PARSE_TIMESTAMP("%Y%m%d",_TABLE_SUFFIX)   ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'Asia/Ho_Chi_Minh'), MONTH, 'Asia/Ho_Chi_Minh'), 'Asia/Ho_Chi_Minh'), INTERVAL -6 MONTH), 'Asia/Ho_Chi_Minh'), 'Asia/Ho_Chi_Minh'), INTERVAL 6 MONTH), 'Asia/Ho_Chi_Minh'))))) 
) t1
LEFT JOIN `batdongsan-datalake-v0.competitor_chotot.dim_category` t2 ON t1.category_id = t2.id
LEFT JOIN `batdongsan-datalake-v0.competitor_chotot.dim_location` t3 ON t1.location_id = t3.id) a
LEFT JOIN `batdongsan-datalake-v0.baonvq.211228_mapping_cate_crawler` b ON a.type_id = b.type_id AND a.cate_name = b.cate_name
LEFT JOIN (SELECT DISTINCT chotot_province, bds_province FROM `batdongsan-datalake-v0.baonvq.province_and_district_map_bds_chotot_alo`) c ON a.province = c.chotot_province
LEFT JOIN (SELECT DISTINCT chotot_province, chotot_district, bds_district FROM `batdongsan-datalake-v0.baonvq.province_and_district_map_bds_chotot_alo`)d ON a.province = d.chotot_province AND a.district = d.chotot_district
LEFT JOIN (SELECT DISTINCT chotot_province, chotot_district, chotot_ward, bds_ward FROM `batdongsan-datalake-v0.baonvq.hcm_hn_district_ward_mapping_bds_chotot_alo`) e ON a.province = e.chotot_province AND a.district = e.chotot_district AND a.ward = e.chotot_ward
WHERE price > 100000000 AND price < 1000000000000
GROUP BY 1,2,3,4,5)
SELECT
    PARSE_DATE("%Y%m",LEFT(chotot_mapping_crawler_211229.date_,6)) AS month_,
    dim_categories.CateName  AS dim_categories_category_name,
    chotot_mapping_crawler_211229.mapped_province  AS chotot_mapping_crawler_211229_chotot_province,
    chotot_mapping_crawler_211229.mapped_district  AS chotot_mapping_crawler_211229_chotot_district,
    chotot_mapping_crawler_211229.mapped_ward  AS chotot_mapping_crawler_211229_chotot_ward,
    CityCode, DistrictId, WardId,
    CASE WHEN COUNT(chotot_mapping_crawler_211229.unitprice) <= 10000 THEN (ARRAY_AGG(chotot_mapping_crawler_211229.unitprice IGNORE NULLS ORDER BY chotot_mapping_crawler_211229.unitprice LIMIT 10000)[OFFSET(CAST(FLOOR(COUNT(chotot_mapping_crawler_211229.unitprice) * 0.5 - 0.0000001) AS INT64))] + ARRAY_AGG(chotot_mapping_crawler_211229.unitprice IGNORE NULLS ORDER BY chotot_mapping_crawler_211229.unitprice LIMIT 10000)[OFFSET(CAST(FLOOR(COUNT(chotot_mapping_crawler_211229.unitprice) * 0.5) AS INT64))]) / 2 ELSE APPROX_QUANTILES(chotot_mapping_crawler_211229.unitprice,1000)[OFFSET(500)] END AS median_of_price_per_square_meter,
    MIN(chotot_mapping_crawler_211229.unitprice) AS min_of_price_per_square_meter,
    MAX(chotot_mapping_crawler_211229.unitprice) AS max_of_price_per_square_meter,
    COUNT(DISTINCT chotot_mapping_crawler_211229.list_id) AS count_of_list_id
FROM chotot_mapping_crawler_211229
LEFT JOIN `batdongsan-datalake-v0.dwh.dim_categories`
     AS dim_categories ON CAST(chotot_mapping_crawler_211229.bds_cate AS int64) = dim_categories.CateId
--LEFT JOIN t4 ON t4.bds_cate_name = chotot_mapping_crawler_211229.bds_cate_name AND t4.mapped_province = chotot_mapping_crawler_211229.mapped_province AND t4.mapped_district = chotot_mapping_crawler_211229.mapped_district AND t4.mapped_ward = chotot_mapping_crawler_211229.mapped_ward
LEFT JOIN t3 ON CONCAT(chotot_mapping_crawler_211229.mapped_province,chotot_mapping_crawler_211229.mapped_district,COALESCE(chotot_mapping_crawler_211229.mapped_ward,"n/a")) = t3.location
LEFT JOIN t4 ON CONCAT(chotot_mapping_crawler_211229.mapped_province,chotot_mapping_crawler_211229.mapped_district,COALESCE(chotot_mapping_crawler_211229.mapped_ward,"n/a")) = CONCAT(t4.mapped_province,t4.mapped_district,COALESCE(t4.mapped_ward,"n/a")) AND t4.bds_cate_name = chotot_mapping_crawler_211229.bds_cate_name AND  LEFT(chotot_mapping_crawler_211229.date_,6) = LEFT(t4.date_,6)
WHERE ((( chotot_mapping_crawler_211229.date  ) >= ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'Asia/Ho_Chi_Minh'), MONTH, 'Asia/Ho_Chi_Minh'), 'Asia/Ho_Chi_Minh'), INTERVAL -6 MONTH), 'Asia/Ho_Chi_Minh'))) AND ( chotot_mapping_crawler_211229.date  ) < ((TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'Asia/Ho_Chi_Minh'), MONTH, 'Asia/Ho_Chi_Minh'), 'Asia/Ho_Chi_Minh'), INTERVAL -6 MONTH), 'Asia/Ho_Chi_Minh'), 'Asia/Ho_Chi_Minh'), INTERVAL 6 MONTH), 'Asia/Ho_Chi_Minh'))))) 
--AND (chotot_mapping_crawler_211229.mapped_province ) IN ('Hà Nội', 'Hồ Chí Minh', 'Bình Dương') 
AND (chotot_mapping_crawler_211229.unitprice ) IS NOT NULL
AND unitprice >= t4.percentile7 AND unitprice <= t4.percentile99
AND dim_categories.CateName IS NOT NULL
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
ORDER BY
    2, count_of_list_id DESC

