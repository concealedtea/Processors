

USE [Reports]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--/****** Object:  Table [dbo].[AdMetrics]    Script Date: 7/5/2018 2:32:58 PM ******/
--DROP TABLE [dbo].[AdMetrics]
--GO


--CREATE TABLE [dbo].[AdMetrics](
--	[timeslice][datetime] NOT NULL, 
--	[date] [date] NOT NULL,
--	[country] [varchar](64) NOT NULL,
--	[push_id] [int] NOT NULL, 
--	[ad_id] [int] NOT NULL, 
--	[advertiser] [varchar](128) NOT NULL,
--	[platform] [varchar] (128) NOT NULL, 
--	[receives] [int] NOT NULL, 
--	[clicks] [int] NOT NULL, 
--	[conversions] [int] NOT NULL,
--	[revenue] [decimal](18, 4) NOT NULL
--) ON [PRIMARY]
--GO

/****** Object:  Index [IX_date_userclass_country_advertiser]    Script Date: 7/5/2018 1:20:12 PM ******/
--CREATE CLUSTERED INDEX [IX_timeslice_date_pushid_country_advertiser_platform] ON [dbo].[AdMetrics]
--(
--	[timeslice] ASC,
--	[date] ASC,
--	[push_id] ASC,
--	[advertiser] ASC, 
--	[platform] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO

--/****** Object:  Index [IX_userclass_date_advertiser]    Script Date: 7/5/2018 1:20:59 PM ******/
--CREATE NONCLUSTERED INDEX [IX_pushid_date_advertiser] ON [dbo].[AdMetrics]
--(
--	[push_id] ASC,
--	[date] ASC,
--	[advertiser] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO


----/****** Object:  Table [dbo].[Upload_UserMetrics]    Script Date: 7/2/2018 5:15:43 PM ******/
--DROP TABLE [dbo].[Upload_AdMetrics]
--GO


--/****** Object:  Table [dbo].[Upload_UserMetrics_Rollup]    Script Date: 7/2/2018 11:41:11 AM ******/
--CREATE TABLE [dbo].[Upload_AdMetrics](
--	[timeslice] [datetime] NOT NULL, 
--	[date] [date] NOT NULL,
--	[country] [varchar](64) NOT NULL,
--	[push_id] [int] NOT NULL, 
--	[ad_id] [int] NOT NULL, 
--	[advertiser] [varchar](128) NOT NULL,
--	[platform] [varchar] (128) NOT NULL, 
--	[receives] [int] NOT NULL, 
--	[clicks] [int] NOT NULL, 
--	[conversions] [int] NOT NULL,
--	[revenue] [decimal](18, 4) NOT NULL
--) ON [PRIMARY]
--GO

ALTER TRIGGER [dbo].[Upload_AdMetrics_Insert] ON [dbo].[Upload_AdMetrics]
   INSTEAD OF INSERT
AS 
BEGIN
	SET NOCOUNT ON;

	    DECLARE @UTC_Offset INT;
		SELECT @UTC_Offset = DATEDIFF(HOUR,GETUTCDATE(),Reports.dbo.get_et());
		MERGE Reports.dbo.AdMetrics AS tgt USING (
		SELECT
			 inserted.[timeslice],
			 inserted.[date],
			 inserted.country, 
			 inserted.push_id,
			 inserted.ad_id,
			 inserted.advertiser,
			 inserted.[platform], 
			 SUM(inserted.receives) AS receives,
			 SUM(inserted.clicks) AS clicks,
			 SUM(inserted.conversions) AS conversions,
			 COALESCE(SUM(COALESCE(DailyRPC.RPC,DailyRPC_Country.RPC) * CAST(inserted.clicks AS FLOAT)),0) AS revenue
		FROM
			inserted AS inserted
		LEFT OUTER JOIN (
			SELECT	
				Date,
				Country, 
				Advertiser, 
				[platform],
				SUM(revenue) / NULLIF(1.0 * SUM(clicks),0) AS RPC
			FROM
				Reports.dbo.DailyRPC
			WHERE 1=1
			GROUP BY
				Date, 
				Country, 
				Advertiser, 
				[platform]
		) AS DailyRPC ON DailyRPC.Date =  CAST(DATEADD(HOUR,@UTC_Offset,inserted.timeslice) AS DATE)
			AND DailyRPC.Country = inserted.country
			AND DailyRPC.Advertiser = inserted.Advertiser
			AND DailyRPC.[platform] = inserted.[platform]
		LEFT OUTER JOIN (
			SELECT
				Date, 
				Country, 
				[platform], 
				SUM(revenue) / NULLIF(1.0 * SUM(clicks),0) AS RPC
			FROM
				Reports.dbo.DailyRPC
			WHERE 1=1
			GROUP BY
				Date, 
				Country, 
				[platform]
		) AS DailyRPC_Country ON DailyRPC_Country.Date =  CAST(DATEADD(HOUR,@UTC_Offset,inserted.timeslice) AS DATE)
			AND DailyRPC_Country.Country = inserted.country
			AND DailyRPC_Country.[platform] = inserted.[platform]
		GROUP BY
			inserted.[timeslice],
			inserted.[date], 
			inserted.country, 
			inserted.advertiser, 
			inserted.[platform], 
			inserted.push_id, 
			inserted.ad_id
	) AS src ON (
		tgt.[timeslice] = src.[timeslice]
		AND tgt.[date] = src.[date]
		AND tgt.country = src.country
		AND tgt.push_id = src.push_id
		AND tgt.ad_id = src.ad_id
		AND tgt.advertiser = src.advertiser
		AND tgt.[platform] = src.[platform]
	)
	WHEN MATCHED THEN UPDATE 
		SET
			tgt.receives = src.receives, 
			tgt.clicks = src.clicks, 
			tgt.conversions = src.conversions,
			tgt.revenue = src.revenue
	WHEN NOT MATCHED BY TARGET THEN 
	INSERT 
	( 
		[timeslice],
		[date],
		country,
		push_id, 
		ad_id,
		advertiser,
		[platform],
		receives,
		clicks,
		conversions,
		revenue
	)
	VALUES
	(
		[timeslice],
		[date],
		country,
		push_id, 
		ad_id,
		advertiser,
		[platform],
		receives,
		clicks,
		conversions,
		revenue
	)
	;
END
GO