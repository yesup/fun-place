package com.yesup.fun;

import com.yesup.as.AddRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 11/07/17.
 */
public class Base64Viewer {

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
        if (args.length == 0 ) {
            LOG.error("missing string as input");
            return;
        }

        byte[] bytes = Base64.getDecoder().decode(args[0]);
        for(int i=0; i<bytes.length; i=i+8) {
            String str = "";
            for (int j=i; j<bytes.length && j<i+8; j++) {
                byte one = bytes[j];
                System.out.print(String.format("%02X", one));
                System.out.print(" ");
                if ( one >= 32 && one < 127 ) {
                    char c = (char)(one);
                    str = str + c;
                } else {
                    str = str + ".";
                }
            }
            if ( bytes.length < i + 8 ) {
                for (int j=0; j < i+8-bytes.length; j++) {
                    System.out.print("   ");
                }
            }
            System.out.println(str);
        }
    }
}
