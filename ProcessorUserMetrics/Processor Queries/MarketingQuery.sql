SELECT
   a.ESTTimestamp
   ,push_id
   , COALESCE(CASE
        WHEN UPPER(subid4) = 'MGID' THEN 'MGID'
        WHEN UPPER(subid4) = 'CONTENTAD' THEN 'ContentAd'
        WHEN page LIKE '%trends.revcontent.com%' THEN 'RevContent'
        ELSE advertiser
    END, 'Unknown') AS advertiser
  ,country
  ,platform
   --,title
   , SUM(CASE WHEN event_name = 'notify_click' THEN 1 ELSE 0 END) AS clicks
   , SUM(CASE WHEN event_name = 'notify_received' THEN 1 ELSE 0 END) AS receives
FROM  --[Impressions].[dbo].[Shard_04]
   ( select
     distinct(user_id)
     ,subid4
     ,page
     --count(*)'count'
     ,event_name
     ,country
     ,platform
     ,push_id
     --,title
     --,date
     ,DATEADD(hour, -4, timestamp)::date "ESTTimestamp"
     from impressions
     where 1=1
       and event_name in( 'notify_received', 'notify_click')
       and DATEADD(hour, -4, timestamp)::date BETWEEN DATEADD(DAY,-3,getdate())::date AND DATEADD(DAY,-1,getdate())::date
       --and date >= '2018-06-18'
       --and push_id in(2128,2127,2126,2125,2103,2102,1922)
       --group by event_name, DATEADD(hour, -4, timestamp)
   )a
  LEFT OUTER JOIN
    schedule ON pushid = push_id
where 1=1
   and event_name in( 'notify_received', 'notify_click')
   --and push_id in(1,2,3,4,5,6,7,8,9,10)
group by
   push_id
  ,country
  ,platform
   --,title
   ,a.ESTTimestamp
  ,COALESCE(CASE
        WHEN UPPER(subid4) = 'MGID' THEN 'MGID'
        WHEN UPPER(subid4) = 'CONTENTAD' THEN 'ContentAd'
        WHEN page LIKE '%trends.revcontent.com%' THEN 'RevContent'
        ELSE advertiser
    END, 'Unknown')
order by
   a.ESTTimestamp desc