package com.yesup.fun;

import com.yesup.grpc.DummyClient;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyGrpcClient {

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }
    }

    public static void main(String[] args) throws Exception {
        try ( DummyClient client = new DummyClient(Constants.REQ_TOTAL, Constants.REQ_QPS) ) {
            client.sendAndWaitComplete();
            client.report();
        }
    }
}
