select * from impressions
where 1=1
  --and ip_address = '24.2.190.218'
  --and country = '--'
  --and timestamp > dateadd(minutes, -240, getdate())
  --and ip_address = '50.234.242.107'
  and timestamp >= DATEADD(DAY,-3,getdate())
  --and platform = 'Desktop'
  --and event_name = 'notify_received'
  --and ip_address = '50.234.242.107'
  --and platform = 'Desktop'
  --and browser_name ilike 'Firefox'
  and imp ilike 'horoscope_microsite'
  and event_name = 'notify_allow'
  --and event_name ilike 'notify_error_prompt_sendtokentoapi'
order by timestamp desc
  --and user_id = '0fb0ff6f-00e7-497e-972a-d0efd4e8b912'
  --and timestamp > '2018-05-29'
  --group by subid10


--ALLOW BY TIMESLICE--
select SUM(CASE WHEN event_name = 'notify_pageview' THEN 1 ELSE 0 END) AS pageviews
       ,SUM(CASE WHEN event_name = 'notify_allow' THEN 1 ELSE 0 END) AS allows
       ,COALESCE(100.0 * SUM(CASE WHEN event_name = 'notify_allow' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN event_name = 'notify_pageview' THEN 1 ELSE 0 END),0),0) AS CVR
       ,dateadd(hour, (datediff(hour, DATE_TRUNC('day',timestamp),timestamp) / 24) * 24, DATE_TRUNC('day',timestamp)) as time
from impressions
where 1=1
 and event_name in ('notify_allow', 'notify_pageview')
 and imp = 'horoscope_microsite'
 and DATEADD(hour, -4, timestamp)::date BETWEEN DATEADD(DAY,-14,getdate())::date AND DATEADD(DAY,0,getdate())::date
group by dateadd(hour, (datediff(hour,DATE_TRUNC('day',timestamp),timestamp) / 24) * 24, DATE_TRUNC('day',timestamp))
order by time asc;



-- count of allows for horoscope_microsite (last 3 days)
select SUM(CASE WHEN event_name = 'notify_allow' THEN 1 ELSE 0 END) AS allows_for_horoscope_microsite,
       SUM(CASE WHEN event_name = 'notify_pageview' THEN 1 ELSE 0 END) AS pageviews_for_horoscope_microsite,
       publisher,
       date
from impressions
where 1=1
  and date >= DATEADD(DAY,-3,getdate())
  and imp ilike 'horoscope_microsite'
group by
  date,
  publisher
order by
  date



select count(*), imp_version, publisher, date
from impressions
where imp ilike 'horoscope_microsite'
  and event_name = 'notify_received'
  and DATEADD(hour, -4, timestamp)::date BETWEEN DATEADD(DAY,-7,getdate())::date AND DATEADD(DAY,0,getdate())::date
group by imp_version, publisher, date
order by date



select SUM(CASE WHEN event_name = 'notify_prompt' THEN 1 ELSE 0 END) AS prompts
      ,SUM(CASE WHEN event_name = 'notify_allow' THEN 1 ELSE 0 END) AS allows
      ,COALESCE(100.0 * SUM(CASE WHEN event_name = 'notify_allow' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN event_name = 'notify_prompt' THEN 1 ELSE 0 END),0),0) AS CVR
      ,dateadd(hour, (datediff(hour ,DATE_TRUNC('day',timestamp),timestamp) / 24) * 24, DATE_TRUNC('day',timestamp)) as time
from impressions
where 1=1
  and imp = 'newtab'
  and event_name in ('notify_allow', 'notify_prompt')
  and browser_name = 'Firefox'
  --and date >= '2018-06-29'
  and timestamp >= '2018-06-08 21:00:00.000000'
group by dateadd(hour, (datediff(hour,DATE_TRUNC('day',timestamp),timestamp) / 24) * 24, DATE_TRUNC('day',timestamp))
  order by time asc;



select *
from impressions
where 1=1
  and date >= DATEADD(DAY,-3,getdate())
  and user_id = '45c9dcaa-2128-44c6-a10d-c9a5a9ca27c6'