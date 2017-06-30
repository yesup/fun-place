package com.yesup.as;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.yesup.fun.Constants;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 30/06/17.
 */
public class AsyncAdd {

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
        EventLoopGroup group = new EpollEventLoopGroup(1);
        EventLoops eventLoops = new NettyEventLoops(group);

        try {
            ClientPolicy policy = new ClientPolicy();
            policy.eventLoops = eventLoops;

            Host[] hosts = Host.parseHosts("192.168.89.38", 3000);

            AerospikeClient client = new AerospikeClient(policy, hosts);

            try {
                EventLoop eventLoop = eventLoops.get(0);

                Key key = new Key("bar", "log", "addkey");

                Bin bin = new Bin("counter", Math.random());

                ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(10);

                client.add(eventLoop, new WriteListener() {
                    @Override
                    public void onSuccess(Key key) {
                        LOG.info("Write success");
                        queue.offer(0);
                    }

                    @Override
                    public void onFailure(AerospikeException e) {
                        LOG.error("Write failed");
                        queue.offer(0);
                    }
                }, null, key, bin);

                queue.take();

            } finally {
                client.close();
            }
        } finally {
            eventLoops.close();
        }


    }
}
