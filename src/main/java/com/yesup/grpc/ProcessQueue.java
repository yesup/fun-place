package com.yesup.grpc;

import com.google.openrtb.OpenRtb;
import com.google.openrtb.json.OpenRtbJsonFactory;
import com.google.protobuf.InvalidProtocolBufferException;
import com.yesup.fun.Constants;
import com.yesup.grpc_dummy.HelloReply;
import com.yesup.grpc_dummy.HelloRequest;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Created by jeffye on 28/06/17.
 */
public class ProcessQueue extends Thread {

    private final static Logger LOG = LoggerFactory.getLogger(ProcessQueue.class);

    static class Item implements Delayed {

        final long insertTs;
        final HelloRequest req;
        final StreamObserver<HelloReply> responseObserver;

        Item(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
            insertTs = System.currentTimeMillis();
            this.req = req;
            this.responseObserver = responseObserver;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = insertTs + Constants.REPLY_DEPLAY - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(insertTs, ((Item) o).insertTs );
        }
    }

    final static DelayQueue<Item> queue = new DelayQueue<>();

    static public void forDelay(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
        queue.put(new Item(req, responseObserver));
    }

    @Override
    public void run() {
        try {
            while (true) {
                Item item = queue.take();

                OpenRtb.BidRequest rtbReq = null;

                try {
                    rtbReq = OpenRtb.BidRequest.parseFrom(item.req.getPayload());
                } catch (InvalidProtocolBufferException e) {
                    LOG.error("Can not parse rtb reqest");
                    return;
                }

                String id = rtbReq == null ? "" : rtbReq.getId();
                String json = "";
                if ( rtbReq == null ) {
                    try {
                        json = OpenRtbJsonFactory.create().newWriter().writeBidRequest(rtbReq);
                    } catch(IOException ex ) {

                    }
                }

                String name = item.req.getName();

                HelloReply.Builder builder = HelloReply.newBuilder();
                builder.setReqId(id);
                builder.setName(name);
                builder.setJson(json);
                builder.setDelay(System.currentTimeMillis() - item.insertTs);

                item.responseObserver.onNext(builder.build());
                item.responseObserver.onCompleted();
            }
        } catch (InterruptedException ex) {

        }
    }
}
