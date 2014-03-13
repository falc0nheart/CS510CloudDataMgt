USE [CloudDataMgt]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[Highway](
	[highwayid] [smallint] NOT NULL,
	[shortdirection] [varchar](1) NULL,
	[direction] [varchar](5) NULL,
	[highwayname] [varchar](6) NULL,
PRIMARY KEY CLUSTERED 
(
	[highwayid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[Stations](
	[stationid] [smallint] PRIMARY KEY CLUSTERED NOT NULL,
	[highwayid] [smallint] NULL,
	[milepost] [real] NULL,
	[locationtext] [varchar](20) NULL,
	[upstream] [smallint] NULL,
	[downstream] [smallint] NULL,
	[stationclass] [smallint] NULL,
	[numberlanes] [smallint] NULL,
	[length_mid ] [real] NULL
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[Detectors](
	[detectorid] [smallint] NOT NULL,
	[highwayid] [smallint] NULL,
	[milepost] [real] NULL,
	[locationtext] [varchar](20) NULL,
	[detectorclass] [smallint] NULL,
	[lanenumber] [smallint] NULL,
	[stationid] [smallint] NULL,
PRIMARY KEY CLUSTERED 
(
	[detectorid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

CREATE TABLE [dbo].[LoopData](
	[detectorid] [smallint] NOT NULL,
	[starttime] [datetime] NOT NULL,
	[volume] [smallint] NULL,
	[speed] [smallint] NULL,
	[occupancy] [smallint] NULL,
	[status] [smallint] NULL,
	[dqflags] [smallint] NULL,
 CONSTRAINT [PK_LoopData] PRIMARY KEY CLUSTERED 
(
	[detectorid] ASC,
	[starttime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO


