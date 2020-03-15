package com.marklogic.kafka.producer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.UriHeader;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class Callback implements org.apache.kafka.clients.producer.Callback {
    private static Logger logger = LoggerFactory.getLogger(Callback.class);

    private static DatabaseClient databaseClient = null;
    private static String targetCollection = null;
    private static String sentCollection = null;

    private Headers headers;

    public static void initializeCallbackClass(DatabaseClient incomingDatabaseClient,
                                               String incomingTargetCollection, String incomingSentCollection) {
        databaseClient = incomingDatabaseClient;
        targetCollection = incomingTargetCollection;
        sentCollection = incomingSentCollection;
    }

    public Callback(Headers headers) {
        this.headers = headers;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            logger.error(String.format("Exception reported for document '%s': %s",
                    new String(headers.lastHeader("URI").value()), exception));
        } else {
            QueryManager queryMgr = databaseClient.newQueryManager();
            Iterator headersIterator = headers.iterator();
            String docUri = new String(headers.lastHeader("URI").value());
            String topicName = new String(headers.lastHeader("TOPIC").value());
            logger.info(String.format("Committed document '%s' to Kafka topic %s", docUri, topicName));
            if (logger.isDebugEnabled()) {
                while (headersIterator.hasNext()) {
                    UriHeader uriHeader = (UriHeader) headersIterator.next();
                    logger.debug("Committed document:");
                    logger.debug("Key: " + uriHeader.key());
                    logger.debug("Value: " + uriHeader.getValue());
                }
            }
            TextDocumentManager textDocumentManager = databaseClient.newTextDocumentManager();
            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            StringHandle stringHandle = new StringHandle();
            String documentContent = textDocumentManager.read(docUri, metadataHandle, stringHandle).get();
            metadataHandle.getCollections().remove(targetCollection);
            metadataHandle.getCollections().add(sentCollection);
            textDocumentManager.writeMetadata(docUri, metadataHandle);
            logger.info(String.format("Collection updated for document '%s', docUri"));
        }
    }
}
