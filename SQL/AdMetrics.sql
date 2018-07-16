-- date, push id, ad_id, clicks, receives, conversions, revenue, country, platform
SELECT
	dateadd(minute, (datediff(minute,DATE_TRUNC('day',"timestamp"),"timestamp") / 15) * 15, DATE_TRUNC('day',"timestamp")) as timeslice
  , date
  , COALESCE(NULLIF(country,''),'--') AS country
  , push_id
  , push_id AS ad_id
  , COALESCE(CASE
  WHEN UPPER(subid4) = 'MGID' THEN 'MGID'
  WHEN UPPER(subid4) = 'CONTENTAD' THEN 'ContentAd'
  WHEN page LIKE '%trends.revcontent.com%' THEN 'RevContent'
  ELSE advertiser
	END, 'Unknown') AS advertiser
  , "platform"
  , SUM(CASE WHEN event_name = 'notify_received' THEN 1 ELSE 0 END) AS receives
  , SUM(CASE WHEN event_name = 'notify_click' THEN 1 ELSE 0 END) AS clicks
  , SUM(CASE WHEN event_name = 'notify_conversion' THEN 1 ELSE 0 END) AS conversions
FROM
  impressions
LEFT OUTER JOIN
  schedule ON pushid = push_id
WHERE 1=1
  AND date BETWEEN '{0}' AND '{1}'
  AND event_name IN ( 'notify_conversion', 'notify_received' , 'notify_click')
GROUP BY
	dateadd(minute, (datediff(minute,DATE_TRUNC('day',"timestamp"),"timestamp") / 15) * 15, DATE_TRUNC('day',"timestamp"))
  , date
	, COALESCE(NULLIF(country,''),'--')
	, "platform"
  , push_id
	, COALESCE(CASE
		WHEN UPPER(subid4) = 'MGID' THEN 'MGID'
		WHEN UPPER(subid4) = 'CONTENTAD' THEN 'ContentAd'
		WHEN page LIKE '%trends.revcontent.com%' THEN 'RevContent'
		ELSE advertiser
	END, 'Unknown')