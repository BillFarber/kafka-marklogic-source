package com.marklogic.kafka.producer;

import com.marklogic.client.UriHeader;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class Callback implements org.apache.kafka.clients.producer.Callback {
    private static Logger logger = LoggerFactory.getLogger(Callback.class);

    Headers headers;

    public Callback(Headers headers) {
        this.headers = headers;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        Iterator headersIterator = headers.iterator();
        logger.info(String.format("Committed document '%s' to Kafka topic %s"
                , new String(headers.lastHeader("URI").value()), new String(headers.lastHeader("TOPIC").value())));
        if (logger.isDebugEnabled()) {
            while (headersIterator.hasNext()) {
                UriHeader uriHeader = (UriHeader) headersIterator.next();
                logger.debug("Committed document:");
                logger.debug("Key: " + uriHeader.key());
                logger.debug("Value: " + uriHeader.getValue());
            }
        }
    }
}
