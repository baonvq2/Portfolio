SELECT explicit_parent, COUNT(keyword)
FROM
(SELECT
    DISTINCT keyword,
    CASE WHEN REGEXP_CONTAINS(keyword, r'(?:^|\W)thue(?:$|\W)') OR REGEXP_CONTAINS(keyword, r'(?:^|\W)thuê(?:$|\W)') THEN 'For Rent'
     WHEN REGEXP_CONTAINS(keyword, r'(?:^|\W)ban(?:$|\W)') OR REGEXP_CONTAINS(keyword, r'(?:^|\W)bán(?:$|\W)') THEN 'For Sale'
     ELSE 'Others' END AS explicit_parent

  FROM
    `batdongsan-datalake-v0.awr.ahref_151121`)
    GROUP BY explicit_parent;
