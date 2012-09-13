package com.yahoo.ycsb.db;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.JSONConverter;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.pbc.RiakClient;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/*
  This is considered pre-alpha, gin-inspired code.
  Use at your own risk. It's currently awaiting review.
*/

public class RiakClient12 extends DB {
    //
    private RawClient rawClient;
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final String RIAK_CLUSTER_HOSTS = "riak_cluster_hosts";
    public static final String RIAK_CLUSTER_HOST_DEFAULT = "127.0.0.1:8087";

    public void init() throws DBException {
        try {
            Properties props = getProperties();
            String cluster_hosts = props.getProperty(RIAK_CLUSTER_HOSTS, RIAK_CLUSTER_HOST_DEFAULT);
            System.err.println(">>>" + cluster_hosts);
            String[] servers = cluster_hosts.split(",");

            if(servers.length == 1) {
                String[] ipAndPort = servers[0].split(":");
                String ip = ipAndPort[0].trim();
                int port = Integer.parseInt(ipAndPort[1].trim());
                System.out.println("Riak connection to " + ip + ":" + port);
                RiakClient riakClient = new RiakClient("127.0.0.1");
                rawClient = new PBClientAdapter(riakClient);
            } else {
                PBClusterConfig clusterConf = new PBClusterConfig(200);
                for(String server:servers) {
                    String[] ipAndPort = server.split(":");
                    String ip = ipAndPort[0].trim();
                    int port = Integer.parseInt(ipAndPort[1].trim());
                    System.out.println("Riak connection to " + ip + ":" + port);
                    PBClientConfig node = PBClientConfig.Builder
                            .from(PBClientConfig.defaults())
                            .withHost(ip)
                            .withPort(port).build();
                    clusterConf.addClient(node);
                }
                rawClient = PBClusterClientFactory.getInstance().newClient(clusterConf);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException("Error connecting to Riak: " + e.getMessage());
        }
    }

    public void cleanup() throws DBException {
        rawClient.shutdown();
    }

    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
            RiakResponse resp = rawClient.fetch(table, key);
            if(resp.hasValue()) {
                IRiakObject obj = resp.getRiakObjects()[0];
                @SuppressWarnings("unchecked")
                HashMap<String, String> m =
                    (HashMap<String, String>)new JSONConverter(HashMap.class, table).toDomain(obj);

                StringByteIterator.putAllAsStrings(m, result);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        // NOT implemented
        return OK;
    }

    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            HashMap<String, String> m = StringByteIterator.getStringMap(values);
            @SuppressWarnings("unchecked")
            IRiakObject obj = new JSONConverter(HashMap.class, table, key).fromDomain(m, null);
            RiakResponse resp = rawClient.fetch(table, key);
            if(resp.hasValue()) {
                IRiakObject robj = resp.getRiakObjects()[0];
                VClock vc = robj.getVClock();
                robj.setValue(obj.getValue());
                rawClient.store(robj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            HashMap<String, String> m = StringByteIterator.getStringMap(values);
            @SuppressWarnings("unchecked")
            IRiakObject obj = new JSONConverter(HashMap.class, table, key).fromDomain(m, null);
            rawClient.store(obj);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int delete(String table, String key) {
        try {
            rawClient.delete(table, key);
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }
}
