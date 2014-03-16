SET STATISTICS TIME ON;
CHECKPOINT;
DBCC DROPCLEANBUFFERS;
DBCC freeproccache;

SELECT  locationtext ,
        TimeInterval ,
        ( [length_mid] / NULLIF(AvgSpeed, 0) ) * 60 [TravelTimeMinutes]
FROM    dbo.Stations
        JOIN ( SELECT   Stations.stationid ,
                        DATEDIFF(minute, '9-22-2011', starttime) / 5 [TimeInterval] ,
                        AVG(speed) [AvgSpeed]
               FROM     dbo.LoopData
                        JOIN dbo.Detectors ON dbo.LoopData.detectorid = dbo.Detectors.detectorid
                        JOIN dbo.Stations ON dbo.Detectors.stationid = dbo.Stations.stationid
                        JOIN dbo.Highway ON dbo.Stations.highwayid = dbo.Highway.highwayid
               WHERE    starttime BETWEEN '9-22-2011'
                                  AND     '9-22-2011 23:59:59'
                        AND shortdirection = 'N'
               GROUP BY Stations.stationid ,
                        DATEDIFF(minute, '9-22-2011', starttime) / 5
             ) a ON dbo.Stations.stationid = a.stationid
ORDER BY dbo.Stations.locationtext ,
        TimeInterval

