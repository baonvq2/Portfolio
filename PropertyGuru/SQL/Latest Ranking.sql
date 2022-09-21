with latest_ranking as (
  select 
    date, 
    keyword, 
    regexp_replace(regexp_replace(url, '(m.batdongsan.com.vn)|(m.duan.batdongsan.com.vn)|(duan.batdongsan.com.vn)|(batdongsan.com.vn)', ''), '(https://)|(http://)', '') url,
    safe_cast(average_monthly_searches as INT64) average_monthly_searches,
    safe_cast(position as INT64) position
  from `awr.ranking` r
  where 1=1
  and website = 'batdongsan.com.vn'
  and regexp_contains(position, r'\d')
  qualify max(date) over(partition by keyword) = r.date
),
awr as (
  select distinct
    r.date as last_awr_update_date,
    r.keyword, 
    first_value(url) over(kw) url,
    first_value(average_monthly_searches) over(kw) average_monthly_searches,
    first_value(position) over(kw) best_position
  from latest_ranking r
  where 1=1
  window kw as (partition by keyword order by safe_cast(position as INT64) asc)
),

ga as (
 select distinct
    ga_hits.hits_pagepath,
    -- ga_hits.content_group,
    case when ga_hits.content_group LIKE '%Search Result%' THEN 'SRP'
    else
    case when ga_hits.content_group = 'Listing Details' THEN 'LDP'
    -- ELSE 
    -- case when ga_hits.content_group = 'Home' THEN 'Home'ELSE 'Other'
    end end landing_page_type,
    first_value(dc.CateName) over(url) cate_name,
    first_value(dl.DistrictName) over(url) district_name,
    first_value(dl.CityName) over(url) city_name,
    first_value(dp.ProjectName) over(url) project_name,
    first_value(ga_hits.custom_SRP_result_count) over(url) srp_result_count,
    first_value(ga.visit_date) over(url) last_visit_date
  from `batdongsan-datalake-v0.derived.ga_master_nested` ga
  left join UNNEST(ga.hits) as ga_hits
  left join `batdongsan-datalake-v0.dwh.dim_categories` dc 
    ON CAST(dc.CateId as STRING) = ga_hits.custom_page_cate_id
  left join (select distinct DistrictId, DistrictName, CityName from `dwh.dim_location`) dl 
    ON  ga_hits.custom_page_district_id = cast(dl.DistrictId as string)
  left join `dwh.dim_projects` dp
    ON ga_hits.custom_page_project_id = cast(dp.ProjectId as string)
  where 1=1
  and ga.visit_date = current_date('Asia/Ho_Chi_Minh') - 1
  window url as (partition by ga_hits.hits_pagepath order by ga_hits.hit_time desc)
)

select 
  awr.*,
  ga.landing_page_type,
  ga.cate_name,
  ga.district_name,
  ga.city_name,
  ga.project_name,
  ga.srp_result_count,
  ga.last_visit_date
from awr
left join ga on awr.url = ga.hits_pagepath
where 1=1
and ga.landing_page_type in ('LDP','SRP')
