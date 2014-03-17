package cass_driver2.cass_driver2;

import com.datastax.driver.core.*;

import java.util.*;

/*
 * Station-to-Station Travel Times: Find travel time 
 * for all station-to-station NB pairs for 8AM on Sept 22, 2011
 */

public class Query4 {
	
	public static void main(String[] args) {
		long startTime = System.nanoTime();
    	Cluster cluster = Cluster.builder()
                .addContactPoints("54.186.36.251")
                .build();
    	Session session = cluster.connect();
    	// Get stationID, lengthMid, DownstreamdStationID
    	ResultSet stationList = getNBStations(session);
    	for (Row row : stationList) {
    		// More queries
    		int castedStation = row.getInt(0);
    		String cqlQuery = "SELECT \"Speed\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    		+ castedStation + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' AND \"StartHour\" = 8 LIMIT 2500;";
    		ResultSet results = session.execute(cqlQuery);
    		
    		// @todo: need to see if downstreamStationID = 0, just compute the one station
    		
    		int castedDownstream = row.getInt(2);
    		String cqlQuery2 = "SELECT \"Speed\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    		+ castedDownstream + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' AND \"StartHour\" = 8 LIMIT 2500;";
    		ResultSet results2 = session.execute(cqlQuery2);
    		
    		int station1SpeedSize = 0; // we'll have to loop through the ResultSet for size
    		int station2SpeedSize = 0; // we'll have to loop through the ResultSet for size
    		int station1SpeedSum = 0;
    		int station2SpeedSum = 0;
    		
    		for(Row row2 : results) {
    			station1SpeedSum += row2.getInt(0);
    			station1SpeedSize++;
    		} // for
    		for(Row row2 : results2) {
    			station1SpeedSum += row2.getInt(0);
    			station2SpeedSize++;
    		} // for
    		
    		// Do averages here
    		String cqlQuery3 = "SELECT \"LengthMid\" FROM \"CloudDataMgt\".\"Station\" WHERE \"StationID\" = " + castedDownstream;
    		ResultSet results3 = session.execute(cqlQuery3);
    		int downstreamLength = 0;
    		for(Row row2 : results3) {
    			downstreamLength = row2.getInt(0); // need to get the downstream lengthMid
    		} // for
    		int lengthMidComb = (row.getInt(1) + downstreamLength) / 2;
    		
    		int station1AvgSpeed = station1SpeedSum / station1SpeedSize;
    		int station2AvgSpeed = station2SpeedSum / station2SpeedSize;
    	} // for
    	long elapsedTime = System.nanoTime() - startTime;
    	double seconds = (double)elapsedTime / 1000000000.0;
    	System.out.println("Query 4 - Done | Elapsed Time in " + seconds + " seconds");
    	System.exit(0);
	}
	
	/*
	 * method: getNBStations
	 * argument: Session session - current Cassandra session
	 * 
	 * This method will return all northbound stations and their
	 * LengthMid in the form of a ResultSet
	 */
	public static ResultSet getNBStations (Session session) {   	
    	// Query Stations
    	String cqlQueryStationNB = "SELECT \"StationID\", \"LengthMid\", \"DownstreamStationID\" FROM \"CloudDataMgt\".\"Stations\" WHERE \"ShortDirection\" = 'N' LIMIT 300;";
    	return session.execute(cqlQueryStationNB);
	} // getNBStations
	
	public static class StationPairs {
		public static int station1;
		public static int station2;
		public static int travelTime;
		
		StationPairs (int s1, int s2, int tt) {
			this.station1 = s1;
			this.station2 = s2;
			this.travelTime = tt;
		} // Constructor
	} // public class StationPairs
} // public class Query 4
