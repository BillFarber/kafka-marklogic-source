package com.marklogic.kafka.producer;

import com.marklogic.client.UriHeader;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Callback implements org.apache.kafka.clients.producer.Callback {
    private static Logger logger = LoggerFactory.getLogger(Callback.class);

    Headers headers;

    public Callback(Headers headers) {
        this.headers = headers;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
//        logger.info("Committed document to Kafka topic: " + ((UriHeader) headers.lastHeader("URI")).getValue());
        logger.info("Committed document to Kafka topic: ");
    }
}
