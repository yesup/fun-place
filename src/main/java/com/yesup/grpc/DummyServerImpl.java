package com.yesup.grpc;

import com.google.common.util.concurrent.RateLimiter;
import com.yesup.fun.Constants;
import com.yesup.grpc_dummy.DummyServerGrpc;
import com.yesup.grpc_dummy.HelloReply;
import com.yesup.grpc_dummy.HelloRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyServerImpl extends DummyServerGrpc.DummyServerImplBase {

    private final static Logger LOG = LoggerFactory.getLogger(DummyServerImpl.class);

    public final static String WARM_UP_NAME = "WARMUP";

    @Override
    public void hello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
        if ( req.getName().equals(WARM_UP_NAME) ) {
            // Slower here, so we can have more threads get started
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            HelloReply reply = HelloReply.newBuilder().build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } else {
            ProcessQueue.forDelay(req, responseObserver);
        }
    }

    public void warmup() {
        Thread warmupThread = new Thread() {
            @Override
            public void run() {
                LOG.info("waiting 2 seconds for doing warm up");
                try {
                    Thread.sleep(2000);
                    doWarmup();
                } catch (InterruptedException e) {
                }

            }
        };
        warmupThread.start();
    }

    void doWarmup() throws InterruptedException {
        LOG.info("warming up");

        long startTs = System.currentTimeMillis();

        HelloRequest req = HelloRequest.newBuilder().setName(WARM_UP_NAME).build();

        EpollEventLoopGroup workerGroup = new EpollEventLoopGroup();
        ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", Constants.PORT)
                .eventLoopGroup(workerGroup)
                .channelType(EpollSocketChannel.class)
                .usePlaintext(true).build();

        DummyServerGrpc.DummyServerStub stub = DummyServerGrpc.newStub(ch);

        RateLimiter limiter = RateLimiter.create(Constants.WARM_UP_QPS);

        ArrayBlockingQueue<Boolean> queue = new ArrayBlockingQueue<>(Constants.WARM_UP_REQ_NUM);

        for(int i=0; i<Constants.WARM_UP_REQ_NUM; i++) {

            limiter.acquire();

            stub.hello(req, new StreamObserver<HelloReply>() {
                @Override
                public void onNext(HelloReply helloReply) {
                    queue.offer(true);
                }

                @Override
                public void onError(Throwable throwable) {
                    queue.offer(false);
                }

                @Override
                public void onCompleted() {

                }
            });
        }

        int done = 0;
        int failed = 0;
        while ( done < Constants.WARM_UP_REQ_NUM ) {
            if ( ! queue.take() ) {
                failed++;
            }
            done++;
        }

        LOG.info("Warm up done after {}ms, failed {}", System.currentTimeMillis() - startTs, failed);
    }
}
