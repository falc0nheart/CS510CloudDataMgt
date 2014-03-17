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
    		String cqlQuery = "SELECT \"Speed\", \"StartHour\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    		+ castedStation + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' LIMIT 125000000;"; 
    		ResultSet results = session.execute(cqlQuery);
    		int castedDownstream = row.getInt(2);
    		// if downstreamStationID = 0
    		if (castedDownstream == 0) {
    			int station1SpeedSize = 1; // we'll have to loop through the ResultSet for size
    			int station1SpeedSum = 0;
 
    			// Loop through Station1
    			for(Row row2 : results) {
    				if (row2.getInt(1) == 8) {
    					station1SpeedSum += row2.getInt(0);
        				station1SpeedSize++;
    				}
    			} // for
    		
    			// Do averages here
    			double lengthMidComb = (row.getDouble(1))/ 2;
    			int station1AvgSpeed = station1SpeedSum / station1SpeedSize;
    			double totalAvgSpeed = station1AvgSpeed;
    			if (totalAvgSpeed < 1) {
    				totalAvgSpeed = 1;
    			}
    			double travelTime = lengthMidComb / totalAvgSpeed;
    			System.out.println ("Station1: " + castedStation + " Station2: 0    | Travel time: " + (travelTime*60) + " in minutes  [downstream was zero]");
    		} // if
    		// if we DO have a downstreamStationID
    		else {
    			String cqlQuery2 = "SELECT \"Speed\", \"StartHour\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    					+ castedDownstream + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' LIMIT 125000000;"; 
    			ResultSet results2 = session.execute(cqlQuery2);
    		
    			int station1SpeedSize = 1; // we'll have to loop through the ResultSet for size
    			int station1SpeedSum = 0;
    		
    			int station2SpeedSize = 1; // we'll have to loop through the ResultSet for size
    			int station2SpeedSum = 0;
    			// Loop through Station1
    			for(Row row2 : results) {
    				if (row2.getInt(1) == 8) {
    					station1SpeedSum += row2.getInt(0);
        				station1SpeedSize++;
    				}
    			} // for
    			// Loop through Station2
    			for(Row row2 : results2) {
    				if (row2.getInt(1) == 8) {
    					station2SpeedSum += row2.getInt(0);
        				station2SpeedSize++;
    				}
    			} // for
    		
    			// Do averages here
    			String cqlQuery3 = "SELECT \"LengthMid\" FROM \"CloudDataMgt\".\"Stations\" WHERE \"StationID\" = " + castedDownstream;
    			ResultSet results3 = session.execute(cqlQuery3);
    			double downstreamLength = 0;
    			for(Row row2 : results3) {
    				downstreamLength = row2.getDouble(0); // need to get the downstream lengthMid
    			} // for
    			double lengthMidComb = (row.getDouble(1) + downstreamLength) / 2;

    			int station1AvgSpeed = station1SpeedSum / station1SpeedSize;
    			int station2AvgSpeed = station2SpeedSum / station2SpeedSize;
    			int totalAvgSpeed = (station1AvgSpeed + station2AvgSpeed) / 2;
    			if (totalAvgSpeed < 1) {
    				totalAvgSpeed = 1;
    			}
    			double travelTime = lengthMidComb / totalAvgSpeed;
    			System.out.println ("Station1: " + castedStation + " Station2: " + castedDownstream + " | Travel time: " + (travelTime * 60) + " in minutes");
    		} // else
    	} // for
    	long elapsedTime = System.nanoTime() - startTime;
    	double seconds = (double)elapsedTime / 1000000000.0;
    	System.out.println("Query 4 - Done | Elapsed Time in " + seconds + " seconds");
    	session.close(); // finish session
    	cluster.close(); // finish cluster connection
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
