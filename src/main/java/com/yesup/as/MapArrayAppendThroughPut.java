package com.yesup.as;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.util.concurrent.RateLimiter;
import com.yesup.fun.Constants;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 04/07/17.
 */
public class MapArrayAppendThroughPut {

    final static int ENTRIES = 10000;
    final static int QPS = 20000;
    final static int TOTAL = 1000000;
    final static int SKIP = 0;

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers()) {
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

                populateEntries(client, eventLoop);

                testing(client, eventLoop);

            } finally {
                client.close();
            }
        } finally {
            eventLoops.close();
        }


    }

    static void populateEntries(AerospikeClient client, EventLoop eventLoop) throws InterruptedException {
        LOG.info("Populate entries");

        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(ENTRIES);

        // starting ending watch thread
        Thread thread = new Thread() {
            @Override
            public void run() {
                int numDone = 0;
                try {
                    while (true) {
                        queue.take();
                        numDone++;
                        if (numDone >= ENTRIES) {
                            break;
                        }

                        if (numDone % 1000 == 0) {
                            LOG.info("Populated {} records", numDone);
                        }
                    }
                } catch (Exception ex) {

                }

            }
        };
        thread.start();

        long startTs = System.currentTimeMillis();

        RateLimiter limiter = RateLimiter.create(1000);

        for (int i = 0; i < ENTRIES; i++) {
            limiter.acquire();
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.expiration = (int)(Math.random() * 3600) + 120;

            Key key = new Key("bar", "map-list-log", "log-" + i);
            Bin name = new Bin("name", "Jeff Ye");
            Bin score = new Bin("id", i);
            Bin cutoff = new Bin("cutoff", System.currentTimeMillis() + 60000);

            client.put(eventLoop, new WriteListener() {
                final long itemStartTs = System.nanoTime();

                @Override
                public void onSuccess(Key key) {
                    update(1);

                }

                @Override
                public void onFailure(AerospikeException e) {
                    LOG.error("Failed {}", e.getMessage());
                    update(0);
                }

                void update(int flag) {
                    queue.offer(flag);
                }
            }, writePolicy, key, name, score, cutoff);
        }

        thread.join();

        LOG.info("All record populated in {}ms", System.currentTimeMillis() - startTs);
    }

    static void testing(AerospikeClient client, EventLoop eventLoop) throws InterruptedException {
        LOG.info("Start testing ...");

        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(TOTAL);

        // starting ending watch thread
        Thread thread = new Thread() {
            @Override
            public void run() {
                int numDone = 0;
                try {
                    while (true) {
                        queue.take();
                        numDone++;
                        if (numDone >= TOTAL) {
                            break;
                        }

                        if (numDone % 1000 == 0) {
                            LOG.info("Updated {} records", numDone);
                        }
                    }
                } catch (Exception ex) {

                }

            }
        };
        thread.start();

        long startTs = System.nanoTime();

        AtomicLong totalCounter = new AtomicLong(0);
        AtomicLong doneCounter = new AtomicLong(0);
        AtomicLong failedCounter = new AtomicLong(0);
        AtomicLong totalTime = new AtomicLong(0);
        AtomicLong slowest = new AtomicLong(0);

        RateLimiter limiter = RateLimiter.create(QPS);

        for (int i = 0; i < TOTAL; i++) {
            limiter.acquire();
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.expiration = (int)(Math.random() * 3600) + 120;

            int randomIndex = (int)(Math.random() * ENTRIES);
            Key key = new Key("bar", "map-list-log", "log-" + randomIndex);

            ArrayList<Value> data = new ArrayList<>();
            HashMap<String, Integer> map = new HashMap<>();
            map.put("test_time", (int)(System.currentTimeMillis()/1000));
            map.put("score",(int)(Math.random() * 10000));
            map.put("test_type", 13);
            data.add(Value.get(map));

            client.operate(eventLoop, new RecordListener() {
                final long itemStartTs = System.nanoTime();

                @Override
                public void onSuccess(Key key, Record record) {
                    doneCounter.incrementAndGet();
                    update(1);
                }

                @Override
                public void onFailure(AerospikeException e) {
                    LOG.error("Failed {}", e.getMessage());
                    failedCounter.incrementAndGet();
                    update(0);
                }

                void update(int flag) {
                    if ( totalCounter.incrementAndGet() > SKIP ) {
                        long elapse = System.nanoTime() - itemStartTs;
                        totalTime.addAndGet(elapse);
                        slowest.updateAndGet((prev) -> elapse > prev ? elapse : prev);
                    }

                    queue.offer(flag);
                }
            }, writePolicy, key, ListOperation.appendItems("tests", data));
        }

        thread.join();

        long totalElapse = System.nanoTime() - startTs;
        long seconds = totalElapse / 1000 / 1000 / 1000;
        LOG.info("Done {}, failed {}, Test finished in {}s, {} updates per second, Avg speed {}us, slowest {}us", new Object[] {
                doneCounter.get(), failedCounter.get(),
                seconds,
                TOTAL / seconds,
                totalTime.get() / 1000 / (TOTAL - SKIP ),
                slowest.get() / 1000
        });
    }
}
