package com.yesup.netty;

import com.yesup.fun.Constants;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Client {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers()) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(Client.class);
    }

    final static Client client = new Client();

    public static void main(String[] args) throws Exception {
        client.sendRequest();
    }

    final EventLoopGroup group = new EpollEventLoopGroup();

    void sendRequest() throws Exception {
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(EpollSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new HttpClientCodec())
                                .addLast(new HttpContentDecompressor())
                                .addLast(new HttpObjectAggregator(1024*1024))
                                .addLast(new ReplyHandler());

                    }
                })
                ;

        Channel ch = b.connect("127.0.0.1", 8080).syncUninterruptibly().channel();

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");

        HttpHeaders headers = req.headers();
        headers.set(HttpHeaderNames.HOST, "127.0.0.1");
        headers.set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

        headers.set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.GZIP);

        String payload = "hello server";

        ByteBuf zipped = Unpooled.buffer(payload.length());

        try (GZIPOutputStream gos = new GZIPOutputStream(new ByteBufOutputStream(zipped));) {
            byte[] bytes = "hello server".getBytes("UTF-8");

            gos.write(bytes, 0, bytes.length);

            gos.finish();
        }

        zipped.resetReaderIndex();
        headers.set(HttpHeaderNames.CONTENT_LENGTH,  zipped.readableBytes());
        req.content().clear().writeBytes(zipped.array());

        zipped.release();

        ch.writeAndFlush(req);
        ch.closeFuture().syncUninterruptibly();

        group.shutdownGracefully();
    }


    class ReplyHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
        @Override
        public void channelRead0(final ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
            String contentEncoding = msg.headers().get(HttpHeaderNames.CONTENT_ENCODING, "");
            if ( ! contentEncoding.isEmpty() ) {
                LOG.info("Content encoding {}", contentEncoding);
            }
            LOG.info("reply {}", msg.content().toString(StandardCharsets.UTF_8));
        }
    }
}
