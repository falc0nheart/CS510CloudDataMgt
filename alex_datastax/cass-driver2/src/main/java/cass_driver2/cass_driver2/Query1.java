package cass_driver2.cass_driver2;

//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Session;
import com.datastax.driver.core.*;

import java.util.*;

public class Query1 {
	
	// Find travel time for each NB station
	// 5-minute intervals
	// limited to 9/22/2011
	
	public static void main(String[] args) {
    	Cluster cluster = Cluster.builder()
                .addContactPoints("54.186.36.251")
                .build();
    	Session session = cluster.connect();    	
    	ArrayList stationList = getNBStations(session);    	
    	System.out.println("Done");
    	System.exit(0);
	}
	
	public static ArrayList getNBStations (Session session) {   	
    	// Query Stations
    	String cqlQueryStationNB = "SELECT \"StationID\" FROM \"CloudDataMgt\".\"Stations\" WHERE \"ShortDirection\" = 'N' LIMIT 300;";
    	ResultSet results = session.execute(cqlQueryStationNB);
    	ArrayList returnedVal = new ArrayList();
    	for(Row row: results) {
    		returnedVal.add(row.getInt(0));
    	}
    	return returnedVal;
	}

}
