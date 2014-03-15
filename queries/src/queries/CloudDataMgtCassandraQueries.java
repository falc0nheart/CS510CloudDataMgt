package queries;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

public class CloudDataMgtCassandraQueries {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Cluster cluster = HFactory.createCluster("TestCluster",  new CassandraHostConfigurator("localhost:9160"));
		Cluster myCluster = HFactory.getOrCreateCluster("CloudDataMgtCluster","54.186.36.251:9160");
//		KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace("CloudDataMgt");
		Keyspace ksp = HFactory.createKeyspace("CloudDataMgt", myCluster);
		System.out.println(myCluster.describeClusterName());
		
//		try {
//	    ColumnFamilyResult<String, String> res = template.queryColumns("a key");
//	    String value = res.getString("domain");
//	    // value should be "www.datastax.com" as per our previous insertion.
//		} catch (HectorException e) {
//	    // do something ...
//		}
	}

}
