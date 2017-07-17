package com.yesup.grpc;

import com.google.common.util.concurrent.RateLimiter;
import com.google.openrtb.OpenRtb;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyClient implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(DummyClient.class);

    final int total;
    final int qps;

    long startTs;
    final long constructTime;

    final EventLoopGroup workerGroup = new EpollEventLoopGroup();

    final AtomicLong totalTime = new AtomicLong(0);
    final AtomicLong totalServerDelay = new AtomicLong(0);
    final AtomicLong slowest = new AtomicLong(0);

    final AtomicInteger doneCounter = new AtomicInteger(0);
    final AtomicInteger errorCounter = new AtomicInteger(0);

    ManagedChannel ch = null;
    DummyServerGrpc.DummyServerStub stub;

    final HelloRequest req;

    public DummyClient(int total, int qps) {
        startTs = System.currentTimeMillis();

        this.total = total;
        this.qps = qps;

        ch = NettyChannelBuilder
                .forAddress("127.0.0.1", Constants.PORT)
                .eventLoopGroup(workerGroup)
                .channelType(EpollSocketChannel.class)
                .usePlaintext(true)
                .flowControlWindow(Constants.flowWindow)
                .build();

        stub = DummyServerGrpc.newStub(ch);

        HelloRequest.Builder builder = HelloRequest.newBuilder();
        builder.setName("Jeff " + Math.random());
        builder.setPayload(createBidRequest().toByteString());
        req = builder.build();

        // send a blocking req
        DummyServerGrpc.DummyServerBlockingStub bStub = DummyServerGrpc.newBlockingStub(ch);
        bStub.hello(req);

        constructTime = System.currentTimeMillis() - startTs;
    }

    public void sendAndWaitComplete() throws Exception {
        LOG.info("start testing now");

        startTs = System.currentTimeMillis();

        RateLimiter limiter = RateLimiter.create(qps, 1, TimeUnit.MINUTES);

        AtomicInteger totalCounter = new AtomicInteger(0);

        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(total);

        for (int i = 0; i < total; i++) {
            limiter.acquire();

            stub.hello(req, new StreamObserver<HelloReply>() {

                final long req_start = System.currentTimeMillis();

                @Override
                public void onNext(HelloReply helloReply) {
                    totalServerDelay.addAndGet(helloReply.getDelay());
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
                    if (replied % 1000 == 0) {
                        LOG.info("{} replied", replied);
                    }

                    long delay = System.currentTimeMillis() - req_start;
                    slowest.updateAndGet((prev) -> delay > prev ? delay : prev);
                    totalTime.addAndGet(delay);
                }
            });

            if (i % 1000 == 0) {
                LOG.info("{} req sent", i);
            }
        }

        int finished = 0;
        while (finished < total) {
            String name = queue.take();
            finished++;
        }


    }

    OpenRtb.BidRequest createBidRequest() {
        OpenRtb.BidRequest.Builder bidBuilder = OpenRtb.BidRequest.newBuilder();

        bidBuilder.setId("test-1111111");

        OpenRtb.BidRequest.Imp.Banner.Builder bannerBuilder = OpenRtb.BidRequest.Imp.Banner.newBuilder();
        bannerBuilder.setId("1");
        bannerBuilder.setW(300);
        bannerBuilder.setH(250);

        bidBuilder.addImp(OpenRtb.BidRequest.Imp.newBuilder().setId("1").setBanner(
                bannerBuilder
        ).setTagid("test-rtb-req"));

        bidBuilder.setSite(
                OpenRtb.BidRequest.Site.newBuilder()
                        .setId("test")
                        .setName("jsmtv.com")
                        .setDomain("jsmtv.com")
                        .setPage("http://www.jsmtv.com")
                        .setPublisher(
                                OpenRtb.BidRequest.Publisher.newBuilder()
                                        .setId("test").setName("Jsmtv")
                        ));
        bidBuilder.setDevice(
                OpenRtb.BidRequest.Device.newBuilder()
                        .setUa("Mozilla/5.0 (Linux; Android 7.0;SAMSUNG SM-G955F Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/5.2 Chrome/51.0.2704.106 Mobile Safari/537.36")
                        .setGeo(OpenRtb.BidRequest.Geo.newBuilder()
                                .setType(OpenRtb.LocationType.IP)
                                .setCountry("USA")
                                .setZip("49221")
                        )
                        .setIp("73.191.234.159")
                        .setDevicetype(OpenRtb.DeviceType.HIGHEND_PHONE)
                        .setOs("Android")
                        .setJs(true)
                        .setConnectiontype(OpenRtb.ConnectionType.WIFI)
        );
        bidBuilder.setUser(
                OpenRtb.BidRequest.User.newBuilder()
                        .setId("testuser")
        );
        bidBuilder.setAt(OpenRtb.AuctionType.SECOND_PRICE);
        bidBuilder.setTmax(125);
        bidBuilder.addCur("USD");

        return bidBuilder.build();
    }

    public void report() {
        long elapse = System.currentTimeMillis() - startTs;
        long throughput = total * 1000 / elapse;
        long speed = totalTime.get() / total;
        long serverDelay = totalServerDelay.get() / total;

        LOG.info("Prepare {}ms, Done {}, error {}, through put {} QPS, speed {}ms, server delay {}ms, slowest {}", new Object[]{
                constructTime,
                doneCounter.get(),
                errorCounter.get(),
                throughput,
                speed,
                serverDelay,
                slowest.get()
        });
    }

    @Override
    public void close() throws IOException {
        try {
            ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        workerGroup.shutdownGracefully().awaitUninterruptibly(2000);
    }
}
