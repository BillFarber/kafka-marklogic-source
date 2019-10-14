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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaMarklogicSource {
	private static Logger logger = LoggerFactory.getLogger(KafkaMarklogicSource.class);

	private static Producer<Long, String> producer;
	private static String DEFAULT_KAFKA_BROKERS = "localhost:9092";
    private static String DEFAULT_TOPIC_NAME="marklogic";
	private static String kafkaBrokers;
	private static String topicName;

	private static DatabaseClient databaseClient;
	private static QueryManager queryMgr;
	private static StringQueryDefinition stringQueryDefinition;
	private static String targetCollection;
	private static String sentCollection;
	private static String query;

    private static Long DELAY=0L;

	private Map<String, String> config;

	public static void main(String[] args) {
		logger.info("Starting kafka-marklogic-source");

		Options options = new Options();

		Option kafkaHostOption = new Option("h", "host", true, "Kafka Host & IP list");
		kafkaHostOption.setRequired(false);
		options.addOption(kafkaHostOption);

		Option topicNameOption = new Option("t", "topic", true, "Topic Name");
		topicNameOption.setRequired(false);
		options.addOption(topicNameOption);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("kafka-marklogic-source", options);

			System.exit(1);
			return;
		}
		kafkaBrokers = cmd.getOptionValue("host", DEFAULT_KAFKA_BROKERS);
		topicName = cmd.getOptionValue("topic", DEFAULT_TOPIC_NAME);

		Properties appProps = new Properties();
		try {
			appProps.load(KafkaMarklogicSource.class.getClassLoader().getResourceAsStream("kafkaSource.properties"));
		} catch (IOException e) {

		}

		String appVersion = appProps.getProperty("version");
		logger.info("kafka-marklogic-source, version: " + appVersion);

		kafkaBrokers = appProps.getProperty(ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS);
		logger.info("host: " + kafkaBrokers);
		topicName = appProps.getProperty(ApplicationConfig.KAFKA_TOPIC);
		logger.info("topic: " + topicName);
		logger.info("MarkLogic Host: " + appProps.getProperty(ApplicationConfig.CONNECTION_HOST));
		logger.info("MarkLogic Port: " + appProps.getProperty(ApplicationConfig.CONNECTION_PORT));
		logger.info("MarkLogic User: " + appProps.getProperty(ApplicationConfig.CONNECTION_USERNAME));

		producer = ProducerCreator.createProducer(kafkaBrokers);

		databaseClient = new DefaultDatabaseClientCreator().createDatabaseClient(appProps);

		queryMgr = databaseClient.newQueryManager();
		query = appProps.getProperty(ApplicationConfig.QUERY_STRING);
		logger.info("Query: " + query);
		stringQueryDefinition = queryMgr.newStringDefinition();
		stringQueryDefinition.setCriteria(query);

		targetCollection = appProps.getProperty(ApplicationConfig.QUERY_TARGET_COLLECTION);
		logger.info("Query Collection: " + targetCollection);
		stringQueryDefinition.setCollections(targetCollection);

		sentCollection = appProps.getProperty(ApplicationConfig.QUERY_SENT_COLLECTION);
		logger.info("Sent Collection: " + sentCollection);

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
}