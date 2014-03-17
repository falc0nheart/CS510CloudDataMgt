DROP KEYSPACE "CloudDataMgt";

CREATE KEYSPACE "CloudDataMgt" WITH replication = { 'class': 'SimpleStrategy' , 'replication_factor' : 2 };

USE "CloudDataMgt";

CREATE TABLE "Stations" (
    "StationID" int,
    "HighwayID" int,
    "DownstreamStationID" int,
    "LocationText" text,
    "LengthMid" double,
    "ShortDirection" text,
    "HighwayName" text,
    PRIMARY KEY ( "StationID")
) WITH COMPACT STORAGE;

CREATE TABLE "LoopData" (
    "StationID" int,
    "HighwayID" int,
    "DetectorID" int,
    "StartDate" TimeStamp,
    "StartTime" text,
    "StartHour" int,
    "StartMinute" int,
    "StartSecond" int,
    "DayOfWeek" text,
    "Volume" int,
    "Speed" int,
  
    PRIMARY KEY ( "StationID","StartDate" , "StartHour", "StartMinute" , "StartSecond")
);

CREATE INDEX ON "LoopData" ("DayOfWeek");
CREATE INDEX ON "LoopData" ("HighwayID");
CREATE INDEX ON "Stations" ("ShortDirection");
CREATE INDEX ON "Stations" ("HighwayName");
