SELECT
	DATEADD(HOUR,{2},"timestamp")::DATE AS revenue_date
	, CASE WHEN event_name IN ( 'notify_prompt' , 'notify_allow' , 'notify_reject' ) THEN date ELSE userclass END userclass
	,"date"
	, COALESCE(NULLIF(country,''),'--') AS country
	, COALESCE(CASE
		WHEN UPPER(subid4) = 'MGID' THEN 'MGID'
		WHEN UPPER(subid4) = 'CONTENTAD' THEN 'ContentAd'
		WHEN page LIKE '%trends.revcontent.com%' THEN 'RevContent'
		ELSE advertiser
	END, 'Unknown') AS advertiser
	, publisher
	, "platform"
	, imp
	, subid1 AS "source"
	, subid2 AS campaign_id
	, browser_name
	, SUM(CASE WHEN event_name = 'notify_prompt' THEN 1 ELSE 0 END) AS prompts
	, SUM(CASE WHEN event_name = 'notify_rejects' THEN 1 ELSE 0 END) AS rejects
	, SUM(CASE WHEN event_name = 'notify_allow' THEN 1 ELSE 0 END) AS allows
	, SUM(CASE WHEN event_name = 'notify_received' THEN 1 ELSE 0 END) AS receives
	, SUM(CASE WHEN event_name = 'notify_click' THEN 1 ELSE 0 END) AS clicks
	, SUM(CASE WHEN event_name = 'notify_close' THEN 1 ELSE 0 END) AS closes
	, SUM(CASE WHEN event_name = 'notify_conversion' THEN 1 ELSE 0 END) AS conversions
FROM (
	SELECT
		"timestamp"
		, userclass
		, "date"
		, country
		, event_name
		, push_id
		, "page"
		, subid4
		, "platform"
		, imp
		, subid1
		, subid2
		, publisher
		, browser_name
	FROM
		impressions
	WHERE 1=1
		AND date BETWEEN '{0}' AND '{1}'
		AND event_name IN ( 'notify_conversion', 'notify_prompt' , 'notify_allow' , 'notify_received' , 'notify_click' )
		AND imp IN ( 'horoscope_microsite' )
) impressions
LEFT OUTER JOIN
	schedule ON pushid = push_id
WHERE 1=1
GROUP BY
	DATEADD(HOUR,{2},"timestamp")::DATE
	, CASE WHEN event_name IN ( 'notify_prompt' , 'notify_allow' , 'notify_reject' ) THEN date ELSE userclass END
	, date
	, COALESCE(NULLIF(country,''),'--')
	, COALESCE(CASE
		WHEN UPPER(subid4) = 'MGID' THEN 'MGID'
		WHEN UPPER(subid4) = 'CONTENTAD' THEN 'ContentAd'
		WHEN page LIKE '%trends.revcontent.com%' THEN 'RevContent'
		ELSE advertiser
	END, 'Unknown')
	, "platform"
	, imp
	, publisher
	, subid1
	, subid2
	, browser_name
ORDER BY
	advertiser