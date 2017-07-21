package com.yesup.as;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.yesup.fun.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 21/07/17.
 */
public final class CountRecords {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(AddRecord.class);
    }

    public static void main(String[] args) throws Exception {
        ClientPolicy policy = new ClientPolicy();

        Host[] hosts = Host.parseHosts("192.168.89.38", 3000);


        AerospikeClient client = new AerospikeClient(policy, hosts);

        try {
            Statement stmt = new Statement();
            stmt.setNamespace("audience");
            stmt.setSetName("audience-ip");
            stmt.setFilter(Filter.equal("tag", "8:test"));
            stmt.setIndexName("audience_tag_index");
            stmt.setAggregateFunction("aggregate", "count");

            ResultSet rs = client.queryAggregate(null, stmt);

            try {

                if ( rs.next() ) {
                    long c = (Long)(rs.getObject());
                    System.out.println("Count " + c);
                } else {
                    System.out.println("no record found");
                }
            } finally {
                rs.close();
            }
        } finally {
            client.close();
        }


    }
}
