package com.marklogic.client;

import com.marklogic.ApplicationConfig;
import com.marklogic.client.ext.ConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.ext.SecurityContextType;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

public class DefaultDatabaseClientCreator implements DatabaseClientCreator {

	private ConfiguredDatabaseClientFactory configuredDatabaseClientFactory;

	public DefaultDatabaseClientCreator() {
		this.configuredDatabaseClientFactory = new DefaultConfiguredDatabaseClientFactory();
	}

	protected DatabaseClientConfig buildDatabaseClientConfig(Properties appProps) {
		DatabaseClientConfig clientConfig = new DatabaseClientConfig();
		clientConfig.setCertFile(appProps.getProperty(ApplicationConfig.CONNECTION_CERT_FILE));
		clientConfig.setCertPassword(appProps.getProperty(ApplicationConfig.CONNECTION_CERT_PASSWORD));

		String type = appProps.getProperty(ApplicationConfig.CONNECTION_TYPE);
		if (type != null && type.trim().length() > 0) {
			clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(type.toUpperCase()));
		}

		String database = appProps.getProperty(ApplicationConfig.QUERY_DATABASE);
		if (database != null && database.trim().length() > 0) {
			clientConfig.setDatabase(database);
		}

		clientConfig.setExternalName(appProps.getProperty(ApplicationConfig.CONNECTION_EXTERNAL_NAME));
		clientConfig.setHost(appProps.getProperty(ApplicationConfig.CONNECTION_HOST));
		clientConfig.setPassword(appProps.getProperty(ApplicationConfig.CONNECTION_PASSWORD));
		clientConfig.setPort(Integer.parseInt(appProps.getProperty(ApplicationConfig.CONNECTION_PORT)));

		String securityContextType = appProps.getProperty(ApplicationConfig.CONNECTION_SECURITY_CONTEXT_TYPE).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));

		String simpleSsl = appProps.getProperty(ApplicationConfig.CONNECTION_SIMPLE_SSL);
		if (simpleSsl != null && Boolean.parseBoolean(simpleSsl)) {
			configureSimpleSsl(clientConfig);
		}

		clientConfig.setUsername(appProps.getProperty(ApplicationConfig.CONNECTION_USERNAME));

		return clientConfig;
	}

	/**
	 * This provides a "simple" SSL configuration in that it uses the JVM's default SSLContext and
	 * a "trust everything" hostname verifier. No default TrustManager is configured because in the absence of one,
	 * the JVM's cacerts file will be used.
	 *
	 * @param clientConfig
	 */
	protected void configureSimpleSsl(DatabaseClientConfig clientConfig) {
		try {
			clientConfig.setSslContext(SSLContext.getDefault());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Unable to get default SSLContext: " + e.getMessage(), e);
		}

		clientConfig.setSslHostnameVerifier((hostname, cns, subjectAlts) -> {
		});
	}

	public void setConfiguredDatabaseClientFactory(ConfiguredDatabaseClientFactory configuredDatabaseClientFactory) {
		this.configuredDatabaseClientFactory = configuredDatabaseClientFactory;
	}

	@Override
	public DatabaseClient createDatabaseClient(Properties appProps) {
		DatabaseClientConfig clientConfig = buildDatabaseClientConfig(appProps);
		return configuredDatabaseClientFactory.newDatabaseClient(clientConfig);
	}
}
