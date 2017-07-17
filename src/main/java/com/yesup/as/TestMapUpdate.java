package com.yesup.as;

import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.yesup.fun.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 17/07/17.
 */
public class TestMapUpdate {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(AppendMap.class);
    }

    public static void main(String[] args) throws Exception {
        ClientPolicy policy = new ClientPolicy();

        Host[] hosts = Host.parseHosts("192.168.89.38", 3000);


        AerospikeClient client = new AerospikeClient(policy, hosts);

        try {
            Key key = new Key("audience", "log", "test_map_update_key2");

            client.operate(null, key, MapOperation.put(MapPolicy.Default, "ddd", Value.get("score3"), Value.get(4)));

        } finally {
            client.close();
        }


    }

}
