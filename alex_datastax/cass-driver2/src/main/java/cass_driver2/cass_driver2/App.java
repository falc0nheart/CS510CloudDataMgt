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
//    	String cqlStatement = "CREATE KEYSPACE myfirstcassandradb WITH " + 
//    			"replication = {'class':'SimpleStrategy','replication_factor':3}";        
//    	session.execute(cqlStatement);
//
//    	String cqlStatement2 = "CREATE TABLE myfirstcassandradb.users (" + 
//    			" user_name varchar PRIMARY KEY," + 
//    			" password varchar " + 
//    			");";
//    	session.execute(cqlStatement2);
    	
    	String cqlStatement3 = "SELECT * FROM sandboxalex.books;";
    	System.out.println(session.execute(cqlStatement3));
    	System.out.println("Done");
    	System.exit(0);
    }
}
