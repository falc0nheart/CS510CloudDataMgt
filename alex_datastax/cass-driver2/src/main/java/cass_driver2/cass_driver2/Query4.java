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
    		String cqlQuery = "SELECT \"StartHour\", \"StartMinute\", \"Speed\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    		+ castedStation + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' AND \"StartHour\" = 8 LIMIT 2500;";
    		ResultSet results = session.execute(cqlQuery);
    		int castedDownstream = row.getInt(2);
    		String cqlQuery2 = "SELECT \"StartHour\", \"StartMinute\", \"Speed\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    		+ castedDownstream + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' AND \"StartHour\" = 8 LIMIT 2500;";
    		ResultSet results2 = session.execute(cqlQuery);
    		// 5-minute intervals
    		HashMap<Integer, List<Integer>> intervalMap = new HashMap<Integer, List<Integer>>();
    		for(int i = 0; i < 287; i++) {
    			intervalMap.put(i, new ArrayList<Integer>());
    		} // for - initialize the List<Integer>
    		for(Row row2 : results) {
    			// Mathematical computation - assigns interval value between 0 - 287
    			int fiveMinInterval = (row2.getInt(0) * 12) + row2.getInt(1)/ 5;
    			// put Speed into HashMap based on 5-minute interval
    			List<Integer> waitwait = intervalMap.get(fiveMinInterval);
    			if (!row2.isNull(2)) {
    				waitwait.add(row2.getInt(2));
    			} // if
    			intervalMap.put(fiveMinInterval, waitwait);
    		} // for
    		// for loop - getting average of intervalMap
    		for(int keyNotAList : intervalMap.keySet()) {
    			List<Integer> waitwait2 = intervalMap.get(keyNotAList);
    			if(!waitwait2.isEmpty()) {
    				int speedSum = 0;
    				for (int i = 0; i < waitwait2.size(); i++) {
    					speedSum += waitwait2.get(i);
    				}
    				newThing.put(keyNotAList, (float) speedSum / waitwait2.size());
    			} // if
    		} // for
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
