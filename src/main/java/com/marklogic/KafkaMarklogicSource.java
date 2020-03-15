package com.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseQuerier;
import com.marklogic.client.DefaultDatabaseClientCreator;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.kafka.producer.Callback;
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
	private static DatabaseQuerier databaseQuerier;

	public static void main(String[] args) {
		logger.info("Starting kafka-marklogic-source");

        Properties appProps = loadProperties(args);
        ApplicationConfig config = new ApplicationConfig(appProps);
        loadConfigurationFromProperties(config);

		SearchHandle results = databaseQuerier.search();

		logger.info("Creating Kafka ProducerRecords from Query results");
		ArrayList<ProducerRecord> records = databaseQuerier.getRecordsFromMatches(results, topicName);

        logger.info("Sending Kafka ProducerRecords");
        sendRecordsToKafka(records);
	}

	private static void sendRecordsToKafka(ArrayList<ProducerRecord> records) {
        for (ProducerRecord record : records) {
            try {
                com.marklogic.kafka.producer.Callback callback = new Callback(record.headers());
                RecordMetadata metadata = (RecordMetadata) producer.send(record, callback).get();
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
		producer = ProducerCreator.createProducer(kafkaBrokers);

        topicName = config.getString(ApplicationConfig.KAFKA_TOPIC);
        logger.info("topic: " + topicName);

        databaseClient = new DefaultDatabaseClientCreator().createDatabaseClient(config);
        databaseQuerier = new DatabaseQuerier(config, databaseClient);
        Callback.initializeCallbackClass(databaseClient,
                config.getString(ApplicationConfig.QUERY_TARGET_COLLECTION),
                config.getString(ApplicationConfig.QUERY_SENT_COLLECTION));
    }

}