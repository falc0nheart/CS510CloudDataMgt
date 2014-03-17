package cass_driver2.cass_driver2;

import com.datastax.driver.core.*;

import java.util.*;

/*
 *  Hourly Corridor Travel Times: Find travel time for the entire I-205 NB freeway section 
 *  in the data set (Sunnyside Rd to the river - all NB stations in the data set) for each 
 *  hour in the 2-month test period
 */

public class Query2 {
	
	public static void main(String[] args) {
		long startTime = System.nanoTime();
    	Cluster cluster = Cluster.builder()
                .addContactPoints("54.186.36.251")
                .build();
    	Session session = cluster.connect();
    	// Get stationID, lengthmid
    	ResultSet stationList = getNBStations(session);
    	// Integer - Hour value
    	// List<Integer> - Set of lists
    	int totalLength = 0;
    	HashMap<Integer, List<Integer>> allStationsHourlyAvg = new HashMap<Integer, List<Integer>>();
    	for(int i = 0; i < 24; i++) {
    		allStationsHourlyAvg.put(i, new ArrayList<Integer>());
		} // for - initialize the List<Integer>
    	for (Row row : stationList) {
    		totalLength = totalLength + (int) row.getDouble(1);
    		// More queries
    		int castedStation = row.getInt(0);
    		String cqlQuery = "SELECT \"StartHour\", \"Speed\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " 
    		+ castedStation + " AND \"HighwayID\" = 3 LIMIT 2500;";
    		// ResultSet contains: (StartDate, StartHour, Speed)
    		ResultSet results = session.execute(cqlQuery);
    		// hourly averages
    		// Integer - Representing hour
    		// List<Integer> - Representing speeds per hour
    		HashMap<Integer, List<Integer>> stationHourlyAvg = new HashMap<Integer, List<Integer>>();
    		for(int i = 0; i < 24; i++) {
    			stationHourlyAvg.put(i, new ArrayList<Integer>());
    		} // for - initialize the List<Integer>
    		for(Row row2 : results) {
    			List<Integer> tempList = stationHourlyAvg.get(row2.getInt(0));
    			tempList.add(row2.getInt(1));
    			stationHourlyAvg.put(row2.getInt(0), tempList);
    		} // for
    		// for loop - getting average of stationHourlyAvg
    		for(int hourValue : stationHourlyAvg.keySet()) {
    			// Get me all SPEED integers for this hour
    			List<Integer> hourSet = stationHourlyAvg.get(hourValue);
    			int speedAvg = 0;
				for (int i = 0; i < hourSet.size(); i++) {
					speedAvg += hourSet.get(i);
				}
				List<Integer> tempList = allStationsHourlyAvg.get(hourValue);
    			tempList.add(speedAvg);
    			allStationsHourlyAvg.put(hourValue, tempList);
    		}
    	} // for
    	// TOTAL AVERAGE
    	for (int hourValue : allStationsHourlyAvg.keySet()) {
    		// Get me all speed averages for each station
    		List<Integer> allStationSpeeds = allStationsHourlyAvg.get(hourValue);
    		int allStationSpeedTotal = 1;
    		for (int i = 0; i < allStationSpeeds.size(); i++) {
    			allStationSpeedTotal += allStationSpeeds.get(i);
    		}
    		// D/S = travel time
    		int travelTime = totalLength / allStationSpeedTotal;
    		System.out.println("HOUR: " + hourValue + " Travel Time: " + travelTime);
    	}
    	long elapsedTime = System.nanoTime() - startTime;
    	double seconds = (double)elapsedTime / 1000000000.0;
    	System.out.println("Query 2 - Done | Elapsed Time in " + seconds + " seconds");
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
    	String cqlQueryStationNB = "SELECT \"StationID\", \"LengthMid\" FROM \"CloudDataMgt\".\"Stations\" WHERE \"ShortDirection\" = 'N' LIMIT 300;";
    	return session.execute(cqlQueryStationNB);
	} // getNBStations
}
