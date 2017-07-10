package com.yesup.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.yesup.as.AddRecord;
import com.yesup.fun.Constants;
import com.yesup.tools.UniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 10/07/17.
 */
public class SendOne {

    private final static Logger LOG;

    static {
        // configure log4j
        org.apache.log4j.PropertyConfigurator.configure(Constants.LOG_CONF);

        // silent java logging of library
        for (Handler h : LogManager.getLogManager().getLogger("").getHandlers() ) {
            h.setLevel(Level.SEVERE);
        }

        LOG = LoggerFactory.getLogger(AddRecord.class);
    }

    public static void main(String[] args) throws Exception {
        Publisher.Builder pubBuilder = Publisher.defaultBuilder(TopicName.create(ServiceOptions.getDefaultProjectId(), "test"));

        FlowControlSettings.Builder fcBuilder = FlowControlSettings.newBuilder();
        fcBuilder.setMaxOutstandingElementCount(10000L);
        fcBuilder.setMaxOutstandingRequestBytes(100L * 1000L * 1000L);
        fcBuilder.setLimitExceededBehavior(FlowController.LimitExceededBehavior.ThrowException);

        BatchingSettings.Builder bhBuilder = BatchingSettings.newBuilder();
        bhBuilder.setDelayThreshold(Duration.ofMillis(100));
        bhBuilder.setElementCountThreshold(500L);
        bhBuilder.setRequestByteThreshold(5L * 1000 * 1000);

        bhBuilder.setFlowControlSettings(fcBuilder.build());

        pubBuilder.setBatchingSettings(bhBuilder.build());

        RetrySettings.Builder rtBuilder = RetrySettings.newBuilder();
        rtBuilder.setTotalTimeout(Duration.ofSeconds(10L));
        rtBuilder.setInitialRetryDelay(Duration.ofMillis(10L));
        rtBuilder.setRetryDelayMultiplier(2);
        rtBuilder.setMaxRetryDelay(Duration.ofSeconds(10));
        rtBuilder.setInitialRpcTimeout(Duration.ofSeconds(1L));
        rtBuilder.setRpcTimeoutMultiplier(2);
        rtBuilder.setMaxRpcTimeout(Duration.ofSeconds(1L));

        pubBuilder.setRetrySettings(rtBuilder.build());


        Publisher publisher = pubBuilder.build();

        Thread thread = new Thread() {
            @Override
            public void run() {
                PageView.Builder payloadBuilder = PageView.newBuilder();
                payloadBuilder.setProperty("abcd");
                payloadBuilder.setSession("dsdwewew");

                PubsubMessage.Builder builder = PubsubMessage.newBuilder();
                builder.setData(payloadBuilder.build().toByteString());

                builder.putAttributes("log_id", UniqueId.getUniqueString());
                builder.putAttributes("log_time", Long.toString(System.currentTimeMillis()));
                builder.putAttributes("logtype", "audience");
                builder.putAttributes("ver", Integer.toString(1));

                PubsubMessage pubsubMessage = builder.build();

                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
                    public void onSuccess(String messageId) {
                        System.out.println("published with message id: " + messageId);
                    }

                    public void onFailure(Throwable t) {
                        System.out.println("failed to publish: " + t);
                    }
                });
            }
        };

        thread.start();

        Thread.sleep(1000);

        publisher.shutdown();
    }
}
