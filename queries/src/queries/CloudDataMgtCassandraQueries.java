package queries;

import java.util.List;

import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createColumnQuery;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMultigetSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Rows;

public class CloudDataMgtCassandraQueries {
	
  private final static String KEYSPACE = "cloudDataMgtTake2";
  private final static String HOST_PORT = "54.186.36.251:9160";
  private final static String CF_NAME = "stations";
  /** Column name where values are stored */
  private final static String COLUMN_NAME = "locationtext";
  private final StringSerializer serializer = StringSerializer.get();
  private final IntegerSerializer intSerializer = IntegerSerializer.get();
  private Keyspace keyspace;

  public static void main(String[] args) throws HectorException {
    Cluster c = HFactory.getOrCreateCluster("MyCluster", HOST_PORT);
    CloudDataMgtCassandraQueries ed = new CloudDataMgtCassandraQueries(HFactory.createKeyspace(KEYSPACE, c));
    //ed.insert("key1", "value1", StringSerializer.get());

    //System.out.println(ed.get("key1", StringSerializer.get()));
    //System.out.println(ed.get(new Integer(1048), IntegerSerializer.get()));
    System.out.println(ed.get("1052", StringSerializer.get()));
  }
  
  public CloudDataMgtCassandraQueries(Keyspace keyspace) {
    this.keyspace = keyspace;
  }
  
  public <K> String get(final K key, Serializer<K> keySerializer) throws HectorException {
    ColumnQuery<K, String, String> q = createColumnQuery(keyspace, keySerializer, serializer, serializer);
    QueryResult<HColumn<String, String>> r = q.setKey(key).
        setName(COLUMN_NAME).
        setColumnFamily(CF_NAME).
        execute();
    HColumn<String, String> c = r.get();
    return c == null ? null : c.getValue();
  }
	  
//  public String get(final Integer key, Serializer<Integer> intSerializer) throws HectorException {
//    ColumnQuery<Integer, String, String> q = HFactory.createColumnQuery(keyspace,
//        intSerializer, serializer, serializer);
//    QueryResult<HColumn<String, String>> r = q.setKey(key).
//        setName(COLUMN_NAME).
//        setColumnFamily(CF_NAME).
//        execute();
//    HColumn<String, String> c = r.get();
//    return c != null ? c.getValue() : null;
//  }
  
//		Cluster myCluster = HFactory.getOrCreateCluster("CloudDataMgtCluster","54.186.36.251:9160");
//		System.out.println("cluster name:  " + myCluster.describeClusterName());
//		
//		KeyspaceDefinition kspDef = myCluster.describeKeyspace("sandboxalex");
//		String kspName = kspDef.getName();
//		System.out.println("keyspace name:  " + kspName);
//		
//		//Keyspace ksp = HFactory.createKeyspace("CloudDataMgt", myCluster);
//		//System.out.println(ksp.getKeyspaceName());
//		
//		
//		List<ColumnFamilyDefinition> lsCf = kspDef.getCfDefs();
//		System.out.println("# of column family names:  " + lsCf.size());
//		System.out.print("column family names:  ");
//		for (int i = 0; i < lsCf.size(); i++) {
//			System.out.print(lsCf.get(i).getName() + "  ");
//		}
//		
////		ColumnFamilyTemplate<String, String> template =
////        new ThriftColumnFamilyTemplate<String, String>(ksp, ,
////                                                       StringSerializer.get(),
////                                                       StringSerializer.get());
////		
////		try {
////	    ColumnFamilyResult<String, String> res = template.queryColumns("a key");
////	    String value = res.getString("domain");
////	    // value should be "www.datastax.com" as per our previous insertion.
////		} catch (HectorException e) {
////	    // do something ...
////		}

}

