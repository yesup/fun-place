package com.yesup.tools;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jeffye on 18/07/17.
 */
public final class SmoothRateLimiter {
    private final static Logger LOG = LoggerFactory.getLogger(SmoothRateLimiter.class);

    final RateLimiter rateLimit;

    volatile Thread changingThread = null;

    public SmoothRateLimiter(double qps) {
        rateLimit = RateLimiter.create(qps);
    }

    public double acquire() {
        return rateLimit.acquire();
    }

    public void changeQps(double newQps, int timeInSecond) {
        // in a thread, make this call non blocking
        Thread thread = new Thread() {
            @Override
            public void run() {
                synchronized (SmoothRateLimiter.this) {
                    if ( changingThread != null ) {
                        changingThread.interrupt();
                        try {
                            changingThread.join();
                        } catch (InterruptedException e) {
                            LOG.warn("exception on waiting changing thread stop");
                        }
                    }
                    if ( timeInSecond < 2 ) {
                        rateLimit.setRate(newQps);
                    } else {
                        changingThread = new ChangingThread(newQps, timeInSecond);
                        changingThread.start();
                    }
                }
            }
        };
        thread.start();
    }

    class ChangingThread extends Thread {

        final double target;
        final int timeInSecond;

        ChangingThread(double newQps, int timeInSecond) {
            target = newQps;
            this.timeInSecond = timeInSecond;
        }

        @Override
        public void run() {
            double start = rateLimit.getRate();
            try {
                for (int i = 0; i < timeInSecond; i++) {
                    double newQps = (target - start) / timeInSecond * i + start;
                    rateLimit.setRate(newQps);
                    Thread.sleep(1000);
                }
                rateLimit.setRate(target);
            } catch (InterruptedException ex) {
                LOG.warn("rate limit changed interrupted");
            }
        }
    }
}
