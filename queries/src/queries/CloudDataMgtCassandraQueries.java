package queries;

import java.util.List;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

public class CloudDataMgtCassandraQueries {

	public static void main(String[] args) {
		Cluster myCluster = HFactory.getOrCreateCluster("CloudDataMgtCluster","54.186.36.251:9160");
		System.out.println("cluster name:  " + myCluster.describeClusterName());
		
		KeyspaceDefinition kspDef = myCluster.describeKeyspace("sandboxalex");
		String kspName = kspDef.getName();
		System.out.println("keyspace name:  " + kspName);
		
		//Keyspace ksp = HFactory.createKeyspace("CloudDataMgt", myCluster);
		//System.out.println(ksp.getKeyspaceName());
		
		
		List<ColumnFamilyDefinition> lsCf = kspDef.getCfDefs();
		System.out.println("# of column family names:  " + lsCf.size());
		System.out.print("column family names:  ");
		for (int i = 0; i < lsCf.size(); i++) {
			System.out.print(lsCf.get(i).getName() + "  ");
		}
		
//		ColumnFamilyTemplate<String, String> template =
//        new ThriftColumnFamilyTemplate<String, String>(ksp, ,
//                                                       StringSerializer.get(),
//                                                       StringSerializer.get());
//		
//		try {
//	    ColumnFamilyResult<String, String> res = template.queryColumns("a key");
//	    String value = res.getString("domain");
//	    // value should be "www.datastax.com" as per our previous insertion.
//		} catch (HectorException e) {
//	    // do something ...
//		}
	}

}
