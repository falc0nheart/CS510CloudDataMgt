package cass_driver2.cass_driver2;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Hector sucks!
 * But maybe DataStax is better!
 */
public class App
{
    public static void main( String[] args )
    {
    	// Cluster cluster = Cluster.builder()
     //            .addContactPoints("54.186.36.251")
     //            .build();
    	// Session session = cluster.connect();
    	// String cqlStatement3 = "SELECT * FROM \"CloudDataMgt\".\"Stations\" LIMIT 5;";
    	// System.out.println(session.execute(cqlStatement3).one());
    	// System.out.println("Done");
    	// System.exit(0);
        Cluster cluster = Cluster.builder()
                .addContactPoints("54.186.36.251")
                .build();
        Session session = cluster.connect();
        String cqlStatement3 = "SELECT * FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = 1052 LIMIT 15;";
        Iterator data = session.execute(cqlStatement3).iterator();
        System.out.println(session.execute(cqlStatement3).getColumnDefinitions());
        for (int i = 0; i < 15; i++) {
            System.out.println("i = " + i + " ==> " + data.next());
        }
        System.out.println("Done");
        System.exit(0);//
    }
}
