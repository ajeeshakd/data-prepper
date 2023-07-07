/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.opensearch.dataprepper.plugins.kafka.util.AuthenticationPropertyConfigurer;
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
import java.util.*;
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
    private final PluginMetrics pluginMetrics;
    private String consumerGroupID;
    private MultithreadedConsumer multithreadedConsumer;
    private int totalWorkers;
    private String pipelineName;
    private String schemaType = MessageFormat.PLAINTEXT.toString();
    private static CachedSchemaRegistryClient schemaRegistryClient;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig, final PluginMetrics pluginMetrics,
                       final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineDescription.getPipelineName();
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        Properties consumerProperties = new Properties();
        setConsumerProperties(consumerProperties);
        sourceConfig.getTopics().forEach(topic -> {
            totalWorkers = 0;
            try {
                totalWorkers = topic.getWorkers();
                consumerGroupID = getGroupId(topic.getName());
                executorService = Executors.newFixedThreadPool(totalWorkers);
                setConsumerTopicProperties(consumerProperties, topic);
                IntStream.range(0, totalWorkers + 1).forEach(index -> {
                    String consumerId = consumerGroupID + "::" + Integer.toString(index + 1);
                    multithreadedConsumer = new MultithreadedConsumer(consumerId,
                            consumerGroupID, consumerProperties, topic, sourceConfig, buffer, pluginMetrics, schemaType);
                    executorService.submit(multithreadedConsumer);
                });
            } catch (Exception e) {
                LOG.error("Failed to setup the Kafka Source Plugin.", e);
                throw new RuntimeException("Failed to setup Kafka Source Plugin...");
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

    private void setConsumerProperties(Properties properties) {
        if (StringUtils.isNotEmpty(sourceConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext())) {
            //protocol = sourceConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext();
        } else if (StringUtils.isNotEmpty(sourceConfig.getAuthConfig().getAuthProtocolConfig().getSsl())) {
            //protocol = sourceConfig.getAuthConfig().getAuthProtocolConfig().getSsl();
        }
        setBoostStrapServerProperties(properties);
        setSchemaRegistryProperties(properties);
        setAuthenticationProperties(properties);
        LOG.info("Starting consumer with the properties : {}", properties);
    }

    private void setAuthenticationProperties(Properties properties) {
        if (sourceConfig.getAuthConfig() != null && sourceConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig() != null) {
            AuthenticationPropertyConfigurer.setSaslPlainTextProperties(sourceConfig, properties);
        } else if (sourceConfig.getAuthConfig() != null && sourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig() != null) {
            AuthenticationPropertyConfigurer.setOauthProperties(sourceConfig, properties);
        }
    }

    private void setSchemaRegistryProperties(Properties properties) {
        if (sourceConfig.getSchemaConfig() != null && StringUtils.isNotEmpty(sourceConfig.getSchemaConfig().getRegistryURL())) {
            setPropertiesForSchemaType(properties);
            setPropertiesForSchemaRegistryConnectivity(properties);
        } else if (sourceConfig.getSchemaConfig() == null) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties);
        }
    }

    private void setBoostStrapServerProperties(Properties properties) {
        String clusterApiKey = sourceConfig.getClusterApiKey();
        String clusterApiSecret = sourceConfig.getClusterApiSecret();
        if (StringUtils.isNotEmpty(clusterApiKey) && StringUtils.isNotEmpty(clusterApiSecret)) {
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + clusterApiKey + "' password='" + clusterApiSecret + "';");
        }
        if (StringUtils.isNotEmpty(sourceConfig.getClientDnsLookup())) {
            properties.put("client.dns.lookup", sourceConfig.getClientDnsLookup());
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
    }

    private void setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(Properties properties) {
        Optional<String> schema = Optional.of(Optional.ofNullable(sourceConfig.getSerdeFormat()).orElse(MessageFormat.PLAINTEXT.toString()));
        schemaType = schema.get();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private void setPropertiesForSchemaType(Properties properties) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        schemaRegistryClient = new CachedSchemaRegistryClient(properties.getProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
                100, propertyMap);
        try {
            schemaType = schemaRegistryClient.getSchemaMetadata(sourceConfig.getTopics().get(0).getName() + "-value",
                    sourceConfig.getSchemaConfig().getVersion()).getSchemaType();
        } catch (IOException | RestClientException e) {
            LOG.error("Unable to find the schema registry details...");
            throw new RuntimeException(e);
        }
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class);
        }
    }

    private void setConsumerTopicProperties(Properties properties, TopicConfig topicConfig) {
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                topicConfig.getAutoCommitInterval().toSecondsPart());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                topicConfig.getAutoOffsetReset());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                topicConfig.getAutoCommit());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        //setConsumerOptionalProperties(properties, topicConfig);
    }

    private void setConsumerOptionalProperties(Properties properties, TopicConfig topicConfig) {
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, topicConfig.getSessionTimeOut());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, topicConfig.getHeartBeatInterval());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, topicConfig.getFetchMaxBytes());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, topicConfig.getFetchMaxWait());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, topicConfig.getMaxPollInterval());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, topicConfig.getConsumerMaxPollRecords());
    }

    private void setPropertiesForSchemaRegistryConnectivity(Properties properties) {
        String schemaRegistryApiKey = sourceConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = sourceConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        if (StringUtils.isNotEmpty(schemaRegistryApiKey) && StringUtils.isNotEmpty(schemaRegistryApiSecret)) {
            String basicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("basic.auth.user.info", basicAuthUserInfo);
        }
        if (StringUtils.isNotEmpty(sourceConfig.getSchemaConfig().getSaslMechanism())) {
            properties.put("sasl.mechanism", sourceConfig.getSchemaConfig().getSaslMechanism());
        }
        if (StringUtils.isNotEmpty(sourceConfig.getSchemaConfig().getSecurityProtocol())) {
            properties.put("security.protocol", sourceConfig.getSchemaConfig().getSecurityProtocol());
        }
        if (StringUtils.isNotEmpty(sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource())) {
            properties.put("basic.auth.credentials.source", sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource());
        }
        properties.put("session.timeout.ms", sourceConfig.getSchemaConfig().getSessionTimeoutms());
    }

    private static boolean validateURL(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return false;
            }

        } catch (URISyntaxException ex) {
            LOG.error("Invalid Schema Registry URI: ", ex);
            return false;
        }
        return true;
    }

    private String getSchemaRegistryUrl() {
        if (StringUtils.isNotEmpty(sourceConfig.getSchemaConfig().getRegistryURL())) {
            return sourceConfig.getSchemaConfig().getRegistryURL();
        }
        return StringUtils.EMPTY;
    }

    private static String getSchemaType(final String registryUrl, final String topicName, final int schemaVersion) {
        StringBuilder response = new StringBuilder();
        String schemaType = MessageFormat.PLAINTEXT.toString();
        try {
            String urlPath = registryUrl + "/subjects/" + topicName + "-value/versions/" + schemaVersion;
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

                if (rootNode.has("schemaType")) {
                    JsonNode node = rootNode.findValue("schemaType");
                    schemaType = node.textValue();
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
            throw new RuntimeException("An error occurred while fetching the schema registry...");
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
