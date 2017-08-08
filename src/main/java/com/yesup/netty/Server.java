package com.yesup.netty;

import com.yesup.fun.Constants;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 08/08/17.
 */
public class Server {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(Server.class);
    }

    final static Server server = new Server();

    public static void main(String[] args) throws Exception {

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
            }
        });
        server.start();
    }

    final EventLoopGroup bossGroup = new EpollEventLoopGroup();
    final EventLoopGroup workerGroup = new EpollEventLoopGroup();

    void start() {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(EpollServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast("idleStateHandler", new IdleStateHandler(5, 0, 0));
                        p.addLast(new HttpServerCodec());
                        p.addLast(new HttpContentDecompressor());
                        p.addLast(new HttpObjectAggregator(1024 * 1024));
                        p.addLast(new HttpContentCompressor());
                        p.addLast(new MyServerHandler());
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                        LOG.error("init channel with error {}", cause.getMessage());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                ;

        int port = 8080;

        ChannelFuture f = b.bind(port).syncUninterruptibly();

        LOG.info("listening on port {}", port);
    }

    void shutdown() {

    }
}
