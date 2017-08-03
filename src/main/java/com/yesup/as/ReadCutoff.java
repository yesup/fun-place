package com.yesup.as;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.yesup.fun.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 03/08/17.
 */
public class ReadCutoff {

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
            long cutoff = System.currentTimeMillis() / 1000 / 60 -1;
            long min = cutoff - 15;
            long max = cutoff;
            LOG.info("Check cut off between {} - {}", min, max);
            Statement stmt = new Statement();
            stmt.setNamespace("audience");
            stmt.setSetName("log");
            stmt.setFilters(Filter.range("cutoff", min, max));
            stmt.setBinNames("id");

            RecordSet rs = client.query(null, stmt);

            try {
                while (rs.next()) {
                    Key key = rs.getKey();
                    Record record = rs.getRecord();
                    String id = record.getString("id");
                    LOG.info("Got {}", id);
                    client.put(null, key, new Bin("cutoff", 0));
                }

            } finally {
                rs.close();
            }
        } finally {
            client.close();
        }
    }
}
