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
    	Cluster cluster = Cluster.builder()
                .addContactPoints("54.186.36.251")
                .build();
    	Session session = cluster.connect();    	
    	String cqlStatement3 = "SELECT * FROM \"CloudDataMgt\".\"Stations\" LIMIT 5;";
    	System.out.println(session.execute(cqlStatement3).one());
    	System.out.println("Done");
    	System.exit(0);
    }
}
