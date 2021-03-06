package com.yesup.as;

import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.ClientPolicy;
import com.yesup.fun.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 30/06/17.
 */
public class AppendRecord {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(AppendRecord.class);
    }

    public static void main(String[] args) throws Exception {
        ClientPolicy policy = new ClientPolicy();

        Host[] hosts = Host.parseHosts("192.168.89.38", 3000);


        AerospikeClient client = new AerospikeClient(policy, hosts);

        try {
            Key key = new Key("bar", "log", "append_key");

            ArrayList<Value> data = new ArrayList<>();
            data.add(Value.get((int)(Math.random() * 10000)));

            client.operate(null, key, ListOperation.appendItems("list", data));
        } finally {
            client.close();
        }


    }
}
