package com.yesup.pubsub;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.yesup.as.AddRecord;
import com.yesup.fun.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.FloatControl;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;

/**
 * Created by jeffye on 10/07/17.
 */
public class Listener {

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
        Subscriber.Builder subBuilder = Subscriber.defaultBuilder(SubscriptionName.create(ServiceOptions.getDefaultProjectId(), "testclient"),
                new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer consumer) {
                        consumer.ack();
                        String type = pubsubMessage.getAttributesOrDefault("logtype", "");
                        LOG.info("receive msg type {}", type);
                        if ( type.equals("audience") ) {
                            try {
                                PageView payload = PageView.parseFrom(pubsubMessage.getData());
                                LOG.info("property {}", payload.getProperty());
                            } catch (InvalidProtocolBufferException e) {
                                LOG.error("can not parse pubsub data");
                            }
                        }
                        LOG.info("receive msg {}", pubsubMessage.getAttributesOrDefault("log_id", ""));
                    }
                }
        );

        FlowControlSettings.Builder fcBuilder = FlowControlSettings.newBuilder();
        fcBuilder.setMaxOutstandingElementCount(100L);
        fcBuilder.setMaxOutstandingRequestBytes(100L * 1000 * 1000);
        subBuilder.setFlowControlSettings(fcBuilder.build());

        Subscriber subscriber = subBuilder.build();

        subscriber.startAsync();

        while(true) {
            Thread.sleep(10000);
        }
    }
}
