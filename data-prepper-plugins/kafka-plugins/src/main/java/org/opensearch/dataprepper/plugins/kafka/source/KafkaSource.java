/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceJsonDeserializer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaSourceConfig sourceConfig;
    private ExecutorService executorService;
    private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
    private final Counter kafkaWorkerThreadProcessingErrors;
    private final PluginMetrics pluginMetrics;
    private String consumerGroupID;
    private MultithreadedConsumer multithreadedConsumer;
    private int totalWorkers;
    private String pipelineName;
    private static String schemaType = "";
    private static final String INSTROSPECT_SERVER_DETAILS = " OAUTH_INTROSPECT_SERVER='"
            + "%s" + "' OAUTH_INTROSPECT_ENDPOINT='" + "%s" + "' " +
            "OAUTH_INTROSPECT_AUTHORIZATION='Basic " + "%s";

    private static final String OAUTH_JAAS_CONFIG = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='"
            + "%s" + "' clientSecret='" + "%s" + "' scope='" + "%s" + "' OAUTH_LOGIN_SERVER='" + "%s" +
            "' OAUTH_LOGIN_ENDPOINT='" + "%s" + "' OAUT_LOGIN_GRANT_TYPE=" + "%s" +
            " OAUTH_LOGIN_SCOPE=%s OAUTH_AUTHORIZATION='Basic " + "%s" + "';";

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig, final PluginMetrics pluginMetrics,
                       final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        sourceConfig.getTopics().forEach(topic -> {
            totalWorkers = 0;
            try {
                Properties consumerProperties = getConsumerProperties(topic);
                totalWorkers = topic.getWorkers();
                consumerGroupID = getGroupId(topic.getName());
                executorService = Executors.newFixedThreadPool(totalWorkers);
                IntStream.range(0, totalWorkers + 1).forEach(index -> {
                    String consumerId = consumerGroupID + "::" + Integer.toString(index + 1);
                    multithreadedConsumer = new MultithreadedConsumer(consumerId,
                            consumerGroupID, consumerProperties, topic, sourceConfig, buffer, pluginMetrics, schemaType);
                    executorService.submit(multithreadedConsumer);
                });
            } catch (Exception e) {
                LOG.error("Failed to setup the Kafka Source Plugin.", e);
                throw new RuntimeException();
            }
        });
    }

    @Override
    public void stop() {
        LOG.info("Shutting down Consumers...");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(
                    calculateLongestThreadWaitingTime(), TimeUnit.MILLISECONDS)) {
                LOG.info("Consumer threads are waiting for shutting down...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during consumer shutdown, exiting uncleanly...");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Consumer shutdown successfully...");
    }

    private String getGroupId(String name) {
        return pipelineName + "::" + name;
    }

    private long calculateLongestThreadWaitingTime() {
        List<TopicConfig> topicsList = sourceConfig.getTopics();
        return topicsList.stream().
                map(
                        topics -> topics.getThreadWaitingTime().toSeconds()
                ).
                max(Comparator.comparingLong(time -> time)).
                orElse(1L);
    }

    private Properties getConsumerProperties(TopicConfig topicConfig) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                topicConfig.getAutoCommitInterval().toSecondsPart());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
        setConsumerOptionalProperties(properties, topicConfig);
        if (sourceConfig.getAuthConfig() != null && sourceConfig.getAuthConfig().getPlainTextAuthConfig() != null) {
            setPropertiesForPlainTextAuth(properties);
        } else if (sourceConfig.getAuthConfig() != null && sourceConfig.getAuthConfig().getoAuthConfig() != null) {
            setPropertiesForOAuth(properties);
        }
        schemaType = getSchemaType(sourceConfig.getSchemaConfig().getRegistryURL(), topicConfig.getName(), sourceConfig.getSchemaConfig().getVersion());
        if (schemaType.isEmpty()) {
            schemaType = MessageFormat.PLAINTEXT.toString();
        }
        System.out.println("\n\tSchema TyPe===========================>"+schemaType);
        setPropertiesForSchemaType(properties, schemaType);
        LOG.info("Starting consumer with the properties : {}", properties);
        return properties;
    }

    private void setConsumerOptionalProperties(Properties properties, TopicConfig topicConfig) {
       /* properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, topicConfig.getSessionTimeOut());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,topicConfig.getHeartBeatInterval());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,topicConfig.getFetchMaxBytes());
        properties.put(ConsumerConfig .FETCH_MAX_WAIT_MS_CONFIG,topicConfig.getFetchMaxWait());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,topicConfig.getMaxPollInterval());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,topicConfig.getConsumerMaxPollRecords());*/
    }

    private void setPropertiesForSchemaType(Properties properties, final String schemaType) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    KafkaAvroDeserializer.class);
            if (validateURL(getSchemaRegistryUrl())) {
                properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
            } else {
                throw new RuntimeException("Invalid Schema Registry URI");
            }
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private static boolean validateURL(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return false;
            }
            return true;
        } catch (URISyntaxException ex) {
            LOG.error("Invalid Schema Registry URI: ", ex);
            return false;
        }
    }

    private String getSchemaRegistryUrl() {
        return sourceConfig.getSchemaConfig().getRegistryURL();
    }

    private void setPropertiesForPlainTextAuth(Properties properties) {
        String username = sourceConfig.getAuthConfig().getPlainTextAuthConfig().getUsername();
        String password = sourceConfig.getAuthConfig().getPlainTextAuthConfig().getPassword();
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
        properties.put("security.protocol", "SASL_PLAINTEXT");
    }

    private void setPropertiesForOAuth(Properties properties) {
        String instrospectProperties = "";
        String oauthClientId = sourceConfig.getAuthConfig().getoAuthConfig().getOauthClientId();
        String oauthClientSecret = sourceConfig.getAuthConfig().getoAuthConfig().getOauthClientSecret();
        String oauthLoginServer = sourceConfig.getAuthConfig().getoAuthConfig().getOauthLoginServer();
        String oauthLoginEndpoint = sourceConfig.getAuthConfig().getoAuthConfig().getOauthLoginEndpoint();
        String oauthLoginGrantType = sourceConfig.getAuthConfig().getoAuthConfig().getOauthLoginGrantType();
        String oauthLoginScope = sourceConfig.getAuthConfig().getoAuthConfig().getOauthLoginScope();
        String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        String oauthIntrospectEndpoint = sourceConfig.getAuthConfig().getoAuthConfig().getOauthIntrospectEndpoint();
        //String tokenEndPointURL = sourceConfig.getAuthConfig().getoAuthConfig().getOauthTokenEndpointURL();
        String tokenEndPointURL = oauthLoginServer.concat(oauthLoginEndpoint);
        String saslMechanism = sourceConfig.getAuthConfig().getoAuthConfig().getOauthSaslMechanism();
        String securityProtocol = sourceConfig.getAuthConfig().getoAuthConfig().getOauthSecurityProtocol();
        String loginCallBackHandler = sourceConfig.getAuthConfig().getoAuthConfig().getOauthSaslLoginCallbackHandlerClass();
        String oauthJwksEndpointURL = sourceConfig.getAuthConfig().getoAuthConfig().getOauthJwksEndpointURL();
        String oauthIntrospectServer = sourceConfig.getAuthConfig().getoAuthConfig().getOauthIntrospectServer();

        properties.put("sasl.mechanism", saslMechanism);
        properties.put("security.protocol", securityProtocol);
        properties.put("sasl.oauthbearer.token.endpoint.url", tokenEndPointURL);
        if (!oauthJwksEndpointURL.isEmpty() || !oauthJwksEndpointURL.isBlank()) {
            properties.put("sasl.oauthbearer.jwks.endpoint.url", oauthJwksEndpointURL);
        }
        properties.put("sasl.login.callback.handler.class", loginCallBackHandler);
        if (oauthIntrospectEndpoint.isBlank() || oauthIntrospectEndpoint.isEmpty()) {
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='" + oauthClientId + "' clientSecret='" + oauthClientSecret + "' scope='" + oauthLoginScope + "' OAUTH_LOGIN_SERVER='" + oauthLoginServer + "' OAUTH_LOGIN_ENDPOINT='" + oauthLoginEndpoint + "' OAUT_LOGIN_GRANT_TYPE=" + oauthLoginGrantType + " OAUTH_LOGIN_SCOPE=" + oauthLoginScope + " OAUTH_AUTHORIZATION='Basic " + oauthAuthorizationToken + "' OAUTH_INTROSPECT_AUTHORIZATION='Basic " + oauthAuthorizationToken + "';");
        } else {
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='" + oauthClientId + "' clientSecret='" + oauthClientSecret + "' scope='" + oauthLoginScope + "' OAUTH_LOGIN_SERVER='" + oauthLoginServer + "' OAUTH_LOGIN_ENDPOINT='" + oauthLoginEndpoint + "' OAUT_LOGIN_GRANT_TYPE=" + oauthLoginGrantType + " OAUTH_LOGIN_SCOPE=" + oauthLoginScope + " OAUTH_AUTHORIZATION='Basic " + oauthAuthorizationToken + "' OAUTH_INTROSPECT_SERVER='" + oauthLoginServer + "' OAUTH_INTROSPECT_ENDPOINT='" + oauthIntrospectEndpoint + "' OAUTH_INTROSPECT_AUTHORIZATION='Basic " + oauthAuthorizationToken + "';");
        }

       /* if (oauthJwksEndpointURL != null && !oauthIntrospectEndpoint.isBlank() && !oauthIntrospectEndpoint.isEmpty()) {
            instrospectProperties = String.format(INSTROSPECT_SERVER_DETAILS, oauthIntrospectServer, oauthIntrospectEndpoint, oauthAuthorizationToken);
        }

        String jassConfig = String.format(OAUTH_JAAS_CONFIG, oauthClientId, oauthClientSecret, oauthLoginScope, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken, instrospectProperties);
*/
    }

    private static String getSchemaType(final String registryUrl, final String topicName, final int schemaVersion) {
        StringBuilder response = new StringBuilder();
        String schemaType = MessageFormat.PLAINTEXT.toString();
        try {
            String urlPath = registryUrl + "subjects/" + topicName + "-value/versions/" + schemaVersion;
            URL url = new URL(urlPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    response.append(inputLine);
                }
                reader.close();
                ObjectMapper mapper = new ObjectMapper();
                Object json = mapper.readValue(response.toString(), Object.class);
                String indented = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                JsonNode rootNode = mapper.readTree(indented);
/*
                if (rootNode.has("schemaType")) {
                    JsonNode node = rootNode.findValue("schemaType");
                    schemaType = node.textValue();
                } else {
                    schemaType = MessageFormat.AVRO.toString();
                }*/

                if (rootNode.has("schemaType")) {
                    schemaType = MessageFormat.JSON.toString();
                } else {
                    schemaType = MessageFormat.AVRO.toString();
                }
            } else {
                InputStream errorStream = connection.getErrorStream();
                String errorMessage = readErrorMessage(errorStream);
                LOG.error("GET request failed while fetching the schema registry details : {}", errorMessage);
            }
        } catch (IOException e) {
            LOG.error("An error while fetching the schema registry details : ", e);
            throw new RuntimeException();
        }
        return schemaType;
    }

    private static String readErrorMessage(InputStream errorStream) throws IOException {
        if (errorStream == null) {
            return null;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
        StringBuilder errorMessage = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            errorMessage.append(line);
        }
        reader.close();
        errorStream.close();
        return errorMessage.toString();
    }
}
