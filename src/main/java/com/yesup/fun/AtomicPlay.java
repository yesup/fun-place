package com.yesup.fun;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 28/06/17.
 */
public class AtomicPlay {

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicLong a = new AtomicLong(13);

        a.updateAndGet(new LongUnaryOperator() {
            @Override
            public long applyAsLong(long operand) {
                System.out.println(operand);
                return 3;
            }
        });

        System.out.println(a.get());
    }
}
