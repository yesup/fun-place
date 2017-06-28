package com.yesup.grpc;

import com.yesup.fun.Constants;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyServer {

    private final static Logger LOG = LoggerFactory.getLogger(DummyServer.class);

    EventLoopGroup bossGroup;
    EventLoopGroup workGroup;
    Server impl;

    ArrayList<ProcessQueue> handlers = new ArrayList<>();

    public void start() throws Exception {
        for(int i=0; i<Constants.QUEUE_HANDLER_NUM; i++) {
            ProcessQueue handler = new ProcessQueue();
            handler.start();
            handlers.add(handler);
        }

        bossGroup = new EpollEventLoopGroup(1);
        workGroup = new EpollEventLoopGroup(16);

        DummyServerImpl serviceHandler = new DummyServerImpl();

        impl = NettyServerBuilder
                .forPort(Constants.PORT)
                .bossEventLoopGroup(bossGroup)
                .workerEventLoopGroup(workGroup)
                .channelType(EpollServerSocketChannel.class)
                .flowControlWindow(Constants.flowWindow)
                .addService(serviceHandler)
                .build();

        impl.start();

        serviceHandler.warmup();

        LOG.info("Listening on {}", Constants.PORT);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                onShutdown();
            }
        });
    }

    void onShutdown() {
        impl.shutdownNow();
        workGroup.shutdownGracefully().awaitUninterruptibly(2000);
        bossGroup.shutdownGracefully().awaitUninterruptibly(2000);
        for(ProcessQueue handler: handlers) {
            handler.interrupt();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        impl.awaitTermination();
    }
}
