package com.yesup.rate;

import com.google.common.util.concurrent.RateLimiter;
import com.yesup.as.AddRecord;
import com.yesup.fun.Constants;
import com.yesup.tools.SmoothRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 18/07/17.
 */
public class CustomWarmUp {

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
        AtomicLong totalQueries = new AtomicLong(0);
        AtomicLong currentQueries = new AtomicLong(0);

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long num = currentQueries.getAndSet(0);
                long qps = num / 5;
                LOG.info("Exec {}, QPS {}", num, qps);
            }
        }, 5* 1000, 5 * 1000);

        SmoothRateLimiter limiter = new SmoothRateLimiter(1000);

        limiter.changeQps(10000, 60);

        Timer delay = new Timer();
        delay.schedule(new TimerTask() {
            @Override
            public void run() {
                limiter.changeQps(500, 15);
            }
        }, 15000L);

        class Worker extends Thread {
            public void run() {
                while(true) {
                    limiter.acquire();
                    currentQueries.incrementAndGet();
                    long total = totalQueries.incrementAndGet();
                    if ( total > 1000 * 10000 ) {
                        break;
                    }
                }
            }
        }

        int numOfWorkers = 2;

        LOG.info("start {} workers", numOfWorkers);

        ArrayList<Worker> workers = new ArrayList<>();
        for(int i=0; i<numOfWorkers; i++) {
            Worker worker = new Worker();
            worker.start();
            workers.add(worker);
        }

        LOG.info("Running");

        for(int i=0; i<workers.size(); i++) {
            workers.get(i).join();
        }

        LOG.info("All worker thread done");

        timer.cancel();
    }
}
