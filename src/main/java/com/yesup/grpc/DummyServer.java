package com.yesup.grpc;

import com.yesup.fun.Constants;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyServer {

    private final static Logger LOG = LoggerFactory.getLogger(DummyServer.class);

    EventLoopGroup bossGroup;
    EventLoopGroup workGroup;
    Server impl;

    public void start() throws Exception {
        bossGroup = new EpollEventLoopGroup(1);
        workGroup = new EpollEventLoopGroup(16);

        impl = NettyServerBuilder
                .forPort(Constants.PORT)
                .bossEventLoopGroup(bossGroup)
                .workerEventLoopGroup(workGroup)
                .channelType(EpollServerSocketChannel.class)
                .addService(new DummyServerImpl())
                .build();

        impl.start();

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
    }

    public void blockUntilShutdown() throws InterruptedException {
        impl.awaitTermination();
    }
}
