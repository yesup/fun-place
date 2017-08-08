package com.yesup.netty;

import com.yesup.pubsub.PageView;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Created by jeffye on 08/08/17.
 */
public class MyServerHandler extends ChannelInboundHandlerAdapter {

    private final static Logger LOG = LoggerFactory.getLogger(MyServerHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        handleChannelRead(ctx, (FullHttpRequest)msg);
    }

    void handleChannelRead(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        String acceptEncoding = req.headers().get(HttpHeaderNames.ACCEPT_ENCODING, "");

        if ( ! acceptEncoding.isEmpty() ) {
            LOG.info("Accept encoding: {}", acceptEncoding);
        }

        if ( req.method().equals(HttpMethod.POST) ) {
            LOG.info("Post request");
            LOG.info("content {}", req.content().toString(StandardCharsets.UTF_8));
        }

        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK
        );

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);

        byte[] data = "Hello client".getBytes();

        response.content().writeBytes(data);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, data.length);

        if ( HttpUtil.isKeepAlive(req) ) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.write(response);
        } else {
            ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        }
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();

        LOG.error("Exception while handle request", cause);

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                LOG.info("receive read idle event, close channel");
                ctx.close();
            }
        }
    }
}
