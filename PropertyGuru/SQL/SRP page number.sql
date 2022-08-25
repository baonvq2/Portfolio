WITH
  t1 AS (
  SELECT
    DISTINCT visit_date,
    full_visitor_id,
    hits_pagepath,
    REGEXP_EXTRACT(CASE
        WHEN REGEXP_CONTAINS(hits_pagepath, r'#') THEN REGEXP_EXTRACT(hits_pagepath, r'(.*)#')
      ELSE
      hits_pagepath
    END
      ,r'(/p\d+$)') AS page_num,
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested`
  LEFT JOIN
    UNNEST (hits) AS hits
  LEFT JOIN
    UNNEST(product) AS product
  WHERE
    visit_date >= "2022-03-28"
    AND visit_date <= "2022-04-03"
    AND page_path_search_type IN ('Scope Search',
      'Mix Search'))
SELECT
  visit_date,
  full_visitor_id,
  REGEXP_REPLACE(CASE
      WHEN REGEXP_CONTAINS(hits_pagepath, r'#') THEN REGEXP_EXTRACT(hits_pagepath, r'(.*)#')
    ELSE
    hits_pagepath
  END
    ,r'(/p\d+$)','') AS hits_pagepath,
  MAX(CASE WHEN CAST(REGEXP_EXTRACT(page_num, r'/p(.*)+$') AS int64) IS NULL THEN 1 ELSE CAST(REGEXP_EXTRACT(page_num, r'/p(.*)+$') AS int64) END) AS page
FROM
  t1
GROUP BY
  visit_date,
  full_visitor_id,
  hits_pagepath

