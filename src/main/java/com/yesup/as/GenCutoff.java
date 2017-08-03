package com.yesup.as;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.yesup.fun.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Random;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 03/08/17.
 */
public class GenCutoff {

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
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.expiration = 15 * 60;

            int n = 10;
            LOG.info("generate {} records", n);
            for (int i=0; i<n; i++) {
                String id = "c-" + i + "-" + (int)(Math.random() * 1000);
                LOG.info("write id {}", id);
                Key key = new Key("audience", "log", id);

                Bin idBin = new Bin("id", id);
                Bin cutoffBin = new Bin("cutoff", System.currentTimeMillis() /1000 / 60 + 2);
                HashMap<String, String> dataMap = new HashMap<>();
                dataMap.put("name", "jeff ye");
                dataMap.put("company", "yesup");
                dataMap.put("time", Long.toString(System.currentTimeMillis()/1000));
                Bin dataBin = new Bin("data", dataMap);

                client.put(writePolicy, key, idBin, cutoffBin, dataBin);
            }
        } finally {
            client.close();
        }


    }
}
