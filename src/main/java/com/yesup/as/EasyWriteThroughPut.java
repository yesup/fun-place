package com.yesup.as;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.util.concurrent.RateLimiter;
import com.yesup.fun.Constants;
import com.yesup.tools.UniqueId;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 30/06/17.
 */
public class EasyWriteThroughPut {

    final static int QPS = 20000;
    final static int TOTAL = 1000000;
    final static int SKIP = 0;

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
                LOG.info("Test with {} records on {} QPS, skip first {} ", new Object[] {TOTAL, QPS, SKIP});

                EventLoop eventLoop = eventLoops.get(0);

                ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(TOTAL);

                Semaphore done = new Semaphore(1);
                done.acquire();

                // starting ending watch thread
                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        int numDone = 0;
                        try {
                            while (true) {
                                queue.take();
                                numDone++;
                                if ( numDone >= TOTAL ) {
                                    break;
                                }

                                if ( numDone % 10000 == 0 ) {
                                    LOG.info("Done {} records", numDone);
                                }
                            }
                        } catch(Exception ex) {

                        } finally {
                            done.release();
                        }

                    }
                };
                thread.start();

                long startTs = System.nanoTime();

                RateLimiter limiter = RateLimiter.create(QPS);

                AtomicLong totalCounter = new AtomicLong(0);
                AtomicLong doneCounter = new AtomicLong(0);
                AtomicLong failedCounter = new AtomicLong(0);
                AtomicLong totalTime = new AtomicLong(0);
                AtomicLong slowest = new AtomicLong(0);

                for (int i=0; i<TOTAL; i++) {
                    limiter.acquire();
                    WritePolicy writePolicy = new WritePolicy();
                    writePolicy.expiration = (int)(Math.random() * 3600) + 120;

                    Key key = new Key("bar", "log", UniqueId.getUniqueString());

                    Bin name = new Bin("name", "Jeff Ye");
                    Bin score = new Bin("score", Math.random() * 100);
                    Bin cutoff = new Bin("cutoff", System.currentTimeMillis() + 60000);

                    client.add(eventLoop, new WriteListener() {
                        final long itemStartTs = System.nanoTime();

                        @Override
                        public void onSuccess(Key key) {
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
                    }, writePolicy, key, name, score, cutoff);
                }

                // block until complete
                done.acquire();
                done.release();

                long totalElapse = System.nanoTime() - startTs;
                long seconds = totalElapse / 1000 / 1000 / 1000;
                LOG.info("Done {}, failed {}, Test finished in {}s, {} inserts per second, Avg speed {}us, slowest {}us", new Object[] {
                        doneCounter.get(), failedCounter.get(),
                        seconds,
                        TOTAL / seconds,
                        totalTime.get() / 1000 / (TOTAL - SKIP ),
                        slowest.get() / 1000
                });
            } finally {
                client.close();
            }
        } finally {
            eventLoops.close();
        }


    }
}
