package com.marklogic.client;

import com.marklogic.ApplicationConfig;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class DatabaseQuerier {
    private static Logger logger = LoggerFactory.getLogger(DatabaseQuerier.class);

    private DatabaseClient databaseClient;
    private QueryManager queryMgr;
    private StringQueryDefinition stringQueryDefinition;
    private String query;
    private String targetCollection = null;

    public DatabaseQuerier(ApplicationConfig config, DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
        queryMgr = databaseClient.newQueryManager();
        query = config.getString(ApplicationConfig.QUERY_STRING);
        logger.info("Query: " + query);
        stringQueryDefinition = queryMgr.newStringDefinition();
        stringQueryDefinition.setCriteria(query);

        targetCollection = config.getString(ApplicationConfig.QUERY_TARGET_COLLECTION);
        logger.info("Query Collection: " + targetCollection);
        stringQueryDefinition.setCollections(targetCollection);
    }

    public SearchHandle search() {
        logger.info("Querying for MarkLogic records");
        SearchHandle results = queryMgr.search(stringQueryDefinition, new SearchHandle());
        logger.info(String.format("Finished querying MarkLogic (%d records found)", results.getTotalResults()));
        return results;
    }

    public ArrayList<ProducerRecord> getRecordsFromMatches(SearchHandle results, String topicName) {
        UriHeader topicHeader = new UriHeader("TOPIC", topicName);
        TextDocumentManager textDocumentManager = databaseClient.newTextDocumentManager();
        ArrayList<ProducerRecord> records = new ArrayList<>();
        MatchDocumentSummary[] summaries = results.getMatchResults();
        Integer offsetCounter = 1;
        for (MatchDocumentSummary summary : summaries ) {
            String docUri = summary.getUri();
            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            StringHandle stringHandle = new StringHandle();
            String documentContent = textDocumentManager.read(docUri, metadataHandle, stringHandle).get();
            ProducerRecord<Long, String> record = new ProducerRecord(topicName, documentContent);
            UriHeader uriHeader = new UriHeader("URI", docUri);
            record.headers().add(uriHeader);
            record.headers().add(topicHeader);
            records.add(record);
        }
        return records;
    }

}
