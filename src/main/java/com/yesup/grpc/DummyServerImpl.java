package com.yesup.grpc;

import com.yesup.grpc_dummy.DummyServerGrpc;
import com.yesup.grpc_dummy.HelloReply;
import com.yesup.grpc_dummy.HelloRequest;
import io.grpc.stub.StreamObserver;

/**
 * Created by jeffye on 27/06/17.
 */
public class DummyServerImpl extends DummyServerGrpc.DummyServerImplBase {

    @Override
    public void hello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
        ProcessQueue.forDelay(req, responseObserver);
    }
}
