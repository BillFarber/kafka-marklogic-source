package com.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DefaultDatabaseClientCreator;
import com.marklogic.client.document.TextDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.kafka.producer.ProducerCreator;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaMarklogicSource {
	private static Logger logger = LoggerFactory.getLogger(KafkaMarklogicSource.class);

	private static Producer<Long, String> producer;
	private static String kafkaBrokers;
	private static String topicName;

	private static DatabaseClient databaseClient;
	private static QueryManager queryMgr;
	private static StringQueryDefinition stringQueryDefinition;
	private static String targetCollection;
	private static String sentCollection;
	private static String query;

	public static void main(String[] args) {
		logger.info("Starting kafka-marklogic-source");

        Properties appProps = loadProperties(args);
        ApplicationConfig config = new ApplicationConfig(appProps);
        loadConfigurationFromProperties(config);

		logger.info("Querying for MarkLogic records");
		SearchHandle results = queryMgr.search(stringQueryDefinition, new SearchHandle());
		logger.info(String.format("Finished querying MarkLogic (%d records found)", results.getTotalResults()));

		logger.info("Sending Kafka ProducerRecord(s)");
		ArrayList<ProducerRecord> records = getRecordsFromMatches(results);
		for (ProducerRecord record : records) {
			try {
				RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
				System.out.println("Record sent to partition " + metadata.partition() + " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}

	private static ArrayList<ProducerRecord> getRecordsFromMatches(SearchHandle results) {
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
			records.add(record);

			metadataHandle.getCollections().remove(targetCollection);
			metadataHandle.getCollections().add(sentCollection);
			textDocumentManager.writeMetadata(docUri, metadataHandle);
		}
		return records;
	}

	private static Properties loadProperties(String[] args) {
        String configFilename = null;

        try {
            if (args.length == 1) {
                configFilename = args[0];
            } else {
                throw new ParseException("The configuration file must be specified.");
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.err.println("kafka-marklogic-source <CONFIG-FILE>");

            System.exit(1);
            return null;
        }

        Properties appProps = new Properties();
        try {
            appProps.load(new FileInputStream(configFilename));
        } catch (IOException e) {
            System.err.println("Config file could not be loaded: " + configFilename);
            System.err.println(e.getMessage());

            System.exit(1);
            return null;
        }
        return appProps;
    }

    private static void loadConfigurationFromProperties(ApplicationConfig config) {
        kafkaBrokers = config.getString(ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS);
        logger.info("host: " + kafkaBrokers);
        topicName = config.getString(ApplicationConfig.KAFKA_TOPIC);
        logger.info("topic: " + topicName);
        logger.info("MarkLogic Host: " + config.getString(ApplicationConfig.CONNECTION_HOST));
        logger.info("MarkLogic Port: " + config.getInt(ApplicationConfig.CONNECTION_PORT));
        logger.info("MarkLogic User: " + config.getString(ApplicationConfig.CONNECTION_USERNAME));

        producer = ProducerCreator.createProducer(kafkaBrokers);

        databaseClient = new DefaultDatabaseClientCreator().createDatabaseClient(config);

        queryMgr = databaseClient.newQueryManager();
        query = config.getString(ApplicationConfig.QUERY_STRING);
        logger.info("Query: " + query);
        stringQueryDefinition = queryMgr.newStringDefinition();
        stringQueryDefinition.setCriteria(query);

        targetCollection = config.getString(ApplicationConfig.QUERY_TARGET_COLLECTION);
        logger.info("Query Collection: " + targetCollection);
        stringQueryDefinition.setCollections(targetCollection);

        sentCollection = config.getString(ApplicationConfig.QUERY_SENT_COLLECTION);
        logger.info("Sent Collection: " + sentCollection);
    }

}