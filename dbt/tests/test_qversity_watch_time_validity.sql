-- Test that all video content has valid watch time data for qversity
select *
from {{ ref('silver_qversity_engagement') }}
where content_type = 'video' 
  and (watch_time_minutes < 0 or watch_time_minutes > 240)  -- Max 4 hours for qversity videos 