package com.marklogic;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Defines configuration properties for the kafka-marklogic-source.
 */
public class ApplicationConfig extends AbstractConfig {

	public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
	public static final String KAFKA_TOPIC = "kafka.topic";

	public static final String CONNECTION_HOST = "ml.connection.host";
	public static final String CONNECTION_PORT = "ml.connection.port";
	public static final String CONNECTION_SECURITY_CONTEXT_TYPE = "ml.connection.securityContextType";
	public static final String CONNECTION_USERNAME = "ml.connection.username";
	public static final String CONNECTION_PASSWORD = "ml.connection.password";
	public static final String CONNECTION_TYPE = "ml.connection.type";
	public static final String CONNECTION_SIMPLE_SSL = "ml.connection.simpleSsl";
	public static final String CONNECTION_CERT_FILE = "ml.connection.certFile";
	public static final String CONNECTION_CERT_PASSWORD = "ml.connection.certPassword";
	public static final String CONNECTION_EXTERNAL_NAME = "ml.connection.externalName";

	public static final String QUERY_TARGET_COLLECTION = "ml.query.targetCollection";
	public static final String QUERY_SENT_COLLECTION = "ml.query.sentCollection";
	public static final String QUERY_DATABASE = "ml.query.database";
	public static final String QUERY_STRING = "ml.query.string";

	public static ConfigDef CONFIG_DEF = new ConfigDef()
		.define(CONNECTION_HOST, Type.STRING, Importance.HIGH, "MarkLogic server hostname")
		.define(CONNECTION_PORT, Type.INT, Importance.HIGH, "The REST app server port to connect to")
		.define(CONNECTION_SECURITY_CONTEXT_TYPE, Type.STRING, Importance.HIGH, "Type of MarkLogic security context to create - either digest, basic, kerberos, certificate, or none")
		.define(CONNECTION_USERNAME, Type.STRING, Importance.HIGH, "Name of MarkLogic user to authenticate as")
		.define(CONNECTION_PASSWORD, Type.STRING, Importance.HIGH, "Password for the MarkLogic user")
		.define(CONNECTION_TYPE, Type.STRING, Importance.LOW, "Connection type; DIRECT or GATEWAY")
		.define(CONNECTION_SIMPLE_SSL, Type.BOOLEAN, Importance.LOW, "Set to true to use a trust-everything SSL connection")
		.define(CONNECTION_CERT_FILE, Type.STRING, Importance.LOW, "Path to a certificate file")
		.define(CONNECTION_CERT_PASSWORD, Type.STRING, Importance.LOW, "Password for the certificate file")
		.define(CONNECTION_EXTERNAL_NAME, Type.STRING, Importance.LOW, "External name for Kerberos authentication")

		.define(QUERY_TARGET_COLLECTION, Type.STRING, Importance.HIGH, "Collection to query for")
		.define(QUERY_SENT_COLLECTION, Type.STRING, Importance.HIGH, "Collection to replace with on the document")
		.define(QUERY_DATABASE, Type.STRING, Importance.LOW, "Database to connect, if different from the one associated with the port")
		.define(QUERY_STRING, Type.STRING, Importance.MEDIUM, "Query string to limit the result set");

	public ApplicationConfig(final Map<?, ?> originals) {
		super(CONFIG_DEF, originals, false);
	}

}
