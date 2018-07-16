SELECT
  CASE WHEN event_name IN (''notify_prompt'', ''notify_allow'', ''notify_reject'')
    THEN date
  ELSE userclass END userclass,
  "date",
  COALESCE(NULLIF(country, ''''), ''--'') AS country
  , "platform"
  , imp
  , imp_version
  , browser_name
  , SUM(CASE WHEN event_name = ''notify_prompt''
    THEN 1
        ELSE 0 END) AS prompts
  , SUM(CASE WHEN event_name = ''notify_rejects''
    THEN 1
        ELSE 0 END) AS rejects
  , SUM(CASE WHEN event_name = ''notify_allow''
    THEN 1
        ELSE 0 END) AS allows
  , SUM(CASE WHEN event_name = ''notify_received''
    THEN 1
        ELSE 0 END) AS receives
  , SUM(CASE WHEN event_name = ''notify_click''
    THEN 1
        ELSE 0 END) AS clicks
  , SUM(CASE WHEN event_name = ''notify_close''
    THEN 1
        ELSE 0 END) AS closes
           FROM (
             SELECT
               userclass,
               DATEADD(HOUR, -4, "timestamp") :: date AS date,
               country,
               event_name,
               push_id,
               "page",
               subid4,
               "platform",
               imp,
               imp_version,
               browser_name
             FROM
               impressions
             WHERE 1 = 1
                   AND date BETWEEN ''2018-06-30'' AND ''2018-07-01''
AND event_name IN ( ''notify_prompt'', ''notify_allow'', ''notify_reject'', ''notify_received'', ''notify_click'', ''notify_close'' )
) impressions
LEFT OUTER JOIN
schedule ON pushid = push_id
WHERE 1=1
GROUP BY
CASE WHEN event_name IN ( ''notify_prompt'', ''notify_allow'', ''notify_reject'' ) THEN date ELSE userclass END
, date
, COALESCE ( NULLIF (country, ''''), ''--'')
, "platform"
, imp
, imp_version
, browser_name
ORDER BY
date