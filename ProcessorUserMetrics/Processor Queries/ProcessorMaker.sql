

USE [Reports]
GO


/****** Object:  Table [dbo].[UserMetrics]    Script Date: 7/5/2018 11:56:36 AM ******/
--DROP TABLE [dbo].[UserMetrics]
--GO

/****** Object:  Table [dbo].[UserMetrics]    Script Date: 7/5/2018 11:56:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

--CREATE TABLE [dbo].[UserMetrics](
--	[userclass] [date] NOT NULL,
--	[date] [date] NOT NULL,
--	[age] [int] NOT NULL,
--	[country] [varchar](64) NOT NULL,
--	[advertiser] [varchar](128) NOT NULL,
--	[platform] [varchar](128) NOT NULL,
--	[imp] [varchar](128) NOT NULL,
--	[browser] [varchar](128) NOT NULL,
--	[prompts] [int] NOT NULL,
--	[rejects] [int] NOT NULL,
--	[allows] [int] NOT NULL,
--	[receives] [int] NOT NULL,
--	[clicks] [int] NOT NULL,
--	[closes] [int] NOT NULL,
--	[conversions] [int] NOT NULL, 
--	[revenue] [decimal](18, 4) NOT NULL,
--	[spend] [decimal](18, 4) NOT NULL,
--) ON [PRIMARY]
--GO

--/****** Object:  Index [IX_date_userclass_country_advertiser]    Script Date: 7/5/2018 1:20:12 PM ******/
--CREATE CLUSTERED INDEX [IX_date_userclass_country_advertiser_platform] ON [dbo].[UserMetrics]
--(
--	[date] ASC,
--	[userclass] ASC,
--	[country] ASC,
--	[advertiser] ASC, 
--	[platform] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO

--/****** Object:  Index [IX_userclass_date_advertiser]    Script Date: 7/5/2018 1:20:59 PM ******/
--CREATE NONCLUSTERED INDEX [IX_userclass_date_advertiser] ON [dbo].[UserMetrics]
--(
--	[userclass] ASC,
--	[date] ASC,
--	[advertiser] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
--GO


/****** Object:  Table [dbo].[Upload_UserMetrics]    Script Date: 7/2/2018 5:15:43 PM ******/
DROP TABLE [dbo].[Upload_UserMetrics]
GO


/****** Object:  Table [dbo].[Upload_UserMetrics_Rollup]    Script Date: 7/2/2018 11:41:11 AM ******/
CREATE TABLE [dbo].[Upload_UserMetrics](
	[userclass] [date] NOT NULL,
	[date] [date] NOT NULL,
	[country] [varchar](64) NOT NULL,
	[advertiser] [varchar](128) NOT NULL,
	[platform] [varchar](128) NOT NULL,
	[imp] [varchar](128) NOT NULL, 
	[browser] [varchar](128) NOT NULL,
	[prompts] [int] NOT NULL,
	[rejects] [int] NOT NULL,
	[allows] [int] NOT NULL,
	[receives] [int] NOT NULL,
	[clicks] [int] NOT NULL,
	[closes] [int] NOT NULL,
	[conversions] [int] NOT NULL, 
	[revenue] [decimal](18, 4) NOT NULL,
	[spend] [decimal](18, 4) NOT NULL
) ON [PRIMARY]
GO

CREATE TRIGGER [dbo].[Upload_UserMetrics_Insert] ON [dbo].[Upload_UserMetrics]
   INSTEAD OF INSERT
AS 
BEGIN
	SET NOCOUNT ON;
		MERGE Reports.dbo.UserMetrics AS tgt USING (
		SELECT
			 inserted.userclass,
			 inserted.[date],
			 DATEDIFF(DAY,inserted.userclass, inserted.[date]) AS age, 
			 inserted.country, 
			 inserted.advertiser,
			 inserted.[platform], 
			 inserted.imp, 
			 inserted.browser,
			 SUM(inserted.prompts) AS prompts, 
			 SUM(inserted.rejects) AS rejects,
			 SUM(inserted.allows) AS allows,
			 SUM(inserted.receives) AS receives,
			 SUM(inserted.clicks) AS clicks,
			 SUM(inserted.closes) AS closes,
			 SUM(inserted.conversions) AS conversions,
			 COALESCE(SUM(COALESCE(DailyRPC.RPC,DailyRPC_Country.RPC) * CAST(inserted.clicks AS FLOAT)),0) AS revenue,
			 0 AS spend
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
		) AS DailyRPC ON DailyRPC.Date = inserted.date
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
		) AS DailyRPC_Country ON DailyRPC_Country.Date = inserted.date
			AND DailyRPC_Country.Country = inserted.country
			AND DailyRPC_Country.[platform] = inserted.[platform]
		GROUP BY
			inserted.userclass, 
			inserted.[date], 
			DATEDIFF(DAY,inserted.userclass, inserted.[date]), 
			inserted.country, 
			inserted.advertiser, 
			inserted.[platform], 
			inserted.[imp],
			inserted.[browser]
	) AS src ON (
		tgt.userclass = src.userclass
		AND tgt.[date] = src.[date]
		AND tgt.country = src.country
		AND tgt.advertiser = src.advertiser
		AND tgt.[platform] = src.[platform]
		AND tgt.imp = src.imp
		AND tgt.browser = src.browser
	)
	WHEN MATCHED THEN UPDATE 
		SET
			tgt.prompts = src.prompts,
			tgt.rejects = src.rejects, 
			tgt.allows = src.allows, 
			tgt.receives = src.receives, 
			tgt.clicks = src.clicks, 
			tgt.closes = src.closes, 
			tgt.conversions = src.conversions,
			tgt.revenue = src.revenue
	WHEN NOT MATCHED BY TARGET THEN 
	INSERT 
	(
		userclass, 
		[date],
		age,
		country,
		advertiser,
		[platform],
		imp, 
		browser,
		prompts, 
		rejects,
		allows, 
		receives,
		clicks,
		closes,
		conversions,
		revenue,
		spend
	)
	VALUES
	(
		src.userclass, 
		src.[date],
		src.age,
		src.country,
		src.advertiser,
		src.[platform],
		src.imp, 
		src.browser,
		src.prompts,
		src.rejects,
		src.allows,
		src.receives,
		src.clicks,
		src.closes,
		src.conversions,
		src.revenue,
		src.spend
	)
	;
END
GO