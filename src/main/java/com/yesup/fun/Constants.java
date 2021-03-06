package com.yesup.fun;

/**
 * Created by jeffye on 27/06/17.
 */
public final class Constants {

    public final static String LOG_CONF = "config/log4j.properties";

    public final static int PORT = 9999;

    public final static int WARM_UP_REQ_NUM = 30000;
    public final static int WARM_UP_QPS = 3000;

    public final static int REQ_TOTAL = 1000000;

    public final static int REQ_QPS = 10000;

    public final static long REPLY_DEPLAY = 100;

    public final static int QUEUE_HANDLER_NUM = 8;

    public final static int flowWindow = 100 * 1024 * 1024;
}
