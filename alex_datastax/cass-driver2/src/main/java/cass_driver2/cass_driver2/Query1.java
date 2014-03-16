package cass_driver2.cass_driver2;

//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Session;
import com.datastax.driver.core.*;

import java.util.*;

public class Query1 {




    public static void main(String[] args) {
        Cluster cluster = Cluster.builder()
                .addContactPoints("54.186.36.251")
                .build();
        Session session = cluster.connect();
        // Find travel time for each NB station
        ResultSet stationList = getNBStations(session);
        for (Row row : stationList) {
            // More queries
            int castedStation = row.getInt(0);
            String cqlQuery = "SELECT \"StartHour\", \"StartMinute\", \"Speed\" FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = "
            + castedStation + " AND \"StartDate\" > '2011-09-22 00:00' AND \"StartDate\" < '2011-09-23 23:59' LIMIT 300;";
            ResultSet results = session.execute(cqlQuery);
            System.out.println(results.all().size());
            // 5-minute intervals

            // put that here.
        } // for
        System.out.println("Query 1 - Done");
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
