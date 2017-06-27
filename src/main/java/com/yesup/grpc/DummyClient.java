package com.yesup.grpc;

import com.google.common.util.concurrent.RateLimiter;
import com.yesup.fun.Constants;
import com.yesup.grpc_dummy.DummyServerGrpc;
import com.yesup.grpc_dummy.HelloReply;
import com.yesup.grpc_dummy.HelloRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyClient implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(DummyClient.class);

    final int total;
    final int qps;

    final EventLoopGroup workerGroup = new EpollEventLoopGroup(16);

    final AtomicInteger doneCounter = new AtomicInteger(0);
    final AtomicInteger errorCounter = new AtomicInteger(0);

    public DummyClient(int total, int qps) {
        this.total = total;
        this.qps = qps;
    }

    public void sendAndWaitComplete() throws Exception {
        ManagedChannel ch = NettyChannelBuilder
                .forAddress("127.0.0.1", Constants.PORT)
                .eventLoopGroup(workerGroup)
                .channelType(EpollSocketChannel.class)
                .usePlaintext(true)
                .build();

        DummyServerGrpc.DummyServerStub stub = DummyServerGrpc.newStub(ch);

        RateLimiter limiter = RateLimiter.create(qps);

        HelloRequest.Builder builder = HelloRequest.newBuilder();
        builder.setName("Jeff");
        HelloRequest req = builder.build();

        AtomicInteger totalCounter = new AtomicInteger(0);


        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(total);

        for(int i=0; i<total; i++) {
            limiter.acquire();
            stub.hello(req, new StreamObserver<HelloReply>() {
                @Override
                public void onNext(HelloReply helloReply) {
                    queue.offer(helloReply.getName());
                    doneCounter.incrementAndGet();
                    progressDisplay();
                }

                @Override
                public void onError(Throwable throwable) {
                    queue.offer("");
                    errorCounter.incrementAndGet();
                    progressDisplay();
                }

                @Override
                public void onCompleted() {

                }

                void progressDisplay() {
                    int replied = totalCounter.incrementAndGet();
                    if ( replied % 1000 == 0 ) {
                        LOG.info("{} replied", replied);
                    }
                }
            });
            if ( i % 1000 == 0 ) {
                LOG.info("{} req sent", i);
            }
        }

        int finished = 0;
        while (finished < total ) {
            String name = queue.take();
            finished++;
        }

        ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
    }

    public void report() {
        LOG.info("Done {}, error {}", doneCounter.get(), errorCounter.get());
    }

    @Override
    public void close() throws IOException {
        workerGroup.shutdownGracefully().awaitUninterruptibly(2000);
    }
}
