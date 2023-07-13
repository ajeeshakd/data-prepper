/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import kafka.common.BrokerEndPointNotAvailableException;
import org.apache.avro.generic.GenericRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.kafka.configuration.*;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
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
import java.util.Set;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.Comparator;
import java.util.Properties;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaSourceConfig sourceConfig;
    private AtomicBoolean shutdownInProgress;
    private ExecutorService executorService;
    private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
    private final Counter kafkaWorkerThreadProcessingErrors;
    private final PluginMetrics pluginMetrics;
    private KafkaSourceCustomConsumer consumer;
    private String pipelineName;
    private String schemaType = MessageFormat.PLAINTEXT.toString();
    private static final String SCHEMA_TYPE = "schemaType";
    private String consumerGroupID;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private static CachedSchemaRegistryClient schemaRegistryClient;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig,
                       final PluginMetrics pluginMetrics,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
        shutdownInProgress = new AtomicBoolean(false);
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        sourceConfig.getTopics().forEach(topic -> {
            MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);
            try {
                Properties consumerProperties = new Properties();
                setConsumerProperties(consumerProperties, topic);
                int numWorkers = topic.getWorkers();
                consumerGroupID = getGroupId(topic.getName());
                setConsumerTopicProperties(consumerProperties, topic);
                executorService = Executors.newFixedThreadPool(numWorkers);
                IntStream.range(0, numWorkers + 1).forEach(index -> {
                    KafkaConsumer kafkaConsumer;
                    switch (schema) {
                        case JSON:
                            kafkaConsumer = new KafkaConsumer<String, JsonNode>(consumerProperties);
                            break;
                        case AVRO:
                            kafkaConsumer = new KafkaConsumer<String, GenericRecord>(consumerProperties);
                            break;
                        case PLAINTEXT:
                        default:
                            kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties);
                            break;
                    }
                    consumer = new KafkaSourceCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topic, schemaType, acknowledgementSetManager, pluginMetrics);

                    executorService.submit(consumer);
                });
            } catch (Exception e) {
                if (e instanceof BrokerNotAvailableException || e instanceof BrokerEndPointNotAvailableException || e instanceof TimeoutException) {
                    LOG.error("The kafka broker is not available...");
                } else {
                    LOG.error("Failed to setup the Kafka Source Plugin.", e);
                }
                throw new RuntimeException();
            }
            LOG.info("Started Kafka source for topic " + topic.getName());
        });
    }

    @Override
    public void stop() {
        LOG.info("Shutting down Consumers...");
        shutdownInProgress.set(true);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(
                    calculateLongestThreadWaitingTime(), TimeUnit.SECONDS)) {
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

    private void setConsumerProperties(Properties properties, TopicConfig topic) {
        setBoostStrapServerProperties(properties, topic);
        setAuthenticationProperties(properties);
        setSchemaRegistryProperties(properties, topic);
        LOG.info("Starting consumer with the properties for Topic {}  : {}", topic.getName(), properties);
    }

    private void setAuthenticationProperties(Properties properties) {
        AuthConfig authConfig = sourceConfig.getAuthConfig();
        if (authConfig != null && authConfig.getAuthMechanismConfig().getPlainTextAuthConfig() != null) {
            KafkaSourceSecurityConfigurer.setSaslPlainTextProperties(sourceConfig, properties);
        } else if (authConfig != null && authConfig.getAuthMechanismConfig().getoAuthConfig() != null) {
            KafkaSourceSecurityConfigurer.setOauthProperties(sourceConfig, properties);
        }
    }

    private void setSchemaRegistryProperties(Properties properties, TopicConfig topic) {
        SchemaConfig schemaConfig = sourceConfig.getSchemaConfig();
        if (schemaConfig != null && StringUtils.isNotEmpty(schemaConfig.getRegistryURL())) {
            setPropertiesForSchemaRegistryConnectivity(properties);
            setPropertiesForSchemaType(properties, topic);
        } else if (schemaConfig == null) {
            setPropertiesForPlaintextAndJsonWithoutSchemaRegistry(properties);
        }
    }

    private void setBoostStrapServerProperties(Properties properties, TopicConfig topic) {
        AuthConfig authConfig = sourceConfig.getAuthConfig();
        if (authConfig != null && authConfig.getAuthMechanismConfig() != null &&
                authConfig.getAuthMechanismConfig().getPlainTextAuthConfig() != null) {
            String clusterApiKey = authConfig.getAuthMechanismConfig().getPlainTextAuthConfig().getClusterApiKey();
            String clusterApiSecret = authConfig.getAuthMechanismConfig().getPlainTextAuthConfig().getClusterApiSecret();
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + clusterApiKey + "' password='" + clusterApiSecret + "';");
        }
        if (StringUtils.isNotEmpty(sourceConfig.getClientDnsLookup())) {
            properties.put("client.dns.lookup", sourceConfig.getClientDnsLookup());
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
        if (!isKafkaClusterExists(sourceConfig.getBootStrapServers())) {
            throw new RuntimeException("The Kafka broker is not available...");
        } /*else {
            if (!isTopicExists(topic)) {
                LOG.error("The Topic {} is not available...", topic.getName());
            }
        }*/
        if (StringUtils.isNotEmpty(sourceConfig.getSslEndpointIdentificationAlgorithm())) {
            properties.put("ssl.endpoint.identification.algorithm", sourceConfig.getSslEndpointIdentificationAlgorithm());
        }
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

    private void setPropertiesForSchemaType(Properties properties, TopicConfig topic) {
        Map prop = properties;
        Map<String, String> propertyMap = (Map<String, String>) prop;
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        schemaRegistryClient = new CachedSchemaRegistryClient(properties.getProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG),
                100, propertyMap);
        try {
            schemaType = schemaRegistryClient.getSchemaMetadata(topic.getName() + "-value",
                    sourceConfig.getSchemaConfig().getVersion()).getSchemaType();
        } catch (IOException | RestClientException e) {
            LOG.error("Failed to connect to the schema registry...");
            throw new RuntimeException(e);
        }
        if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        } else if (schemaType.equalsIgnoreCase(MessageFormat.AVRO.toString())) {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
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
        //(properties, topicConfig);
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
        AuthConfig authConfig = sourceConfig.getAuthConfig();
        String schemaRegistryApiKey = sourceConfig.getSchemaConfig().getSchemaRegistryApiKey();
        String schemaRegistryApiSecret = sourceConfig.getSchemaConfig().getSchemaRegistryApiSecret();
        //with plaintext authentication
        if ("USER_INFO".equalsIgnoreCase(sourceConfig.getSchemaConfig().getBasicAuthCredentialsSource())
                && authConfig.getAuthMechanismConfig().getPlainTextAuthConfig() != null) {
            String schemaBasicAuthUserInfo = schemaRegistryApiKey.concat(":").concat(schemaRegistryApiSecret);
            properties.put("schema.registry.basic.auth.user.info", schemaBasicAuthUserInfo);
            properties.put("basic.auth.credentials.source", "USER_INFO");
        }

        if (authConfig != null &&
                authConfig.getAuthMechanismConfig().getPlainTextAuthConfig() != null) {
            String sasl_mechanism = authConfig.getAuthMechanismConfig().getPlainTextAuthConfig().getSaslMechanism();
            properties.put("sasl.mechanism", sasl_mechanism);
        } else if (authConfig != null &&
                authConfig.getAuthMechanismConfig().getoAuthConfig() != null) {
            properties.put("sasl.mechanism", authConfig.getAuthMechanismConfig().getoAuthConfig().getOauthSaslMechanism());
        }


        if (authConfig != null &&
                authConfig.getAuthProtocolConfig().getPlaintext() != null) {
            String protocol = authConfig.getAuthProtocolConfig().getPlaintext();
            properties.put("security.protocol", protocol);
        } else if (authConfig != null &&
                authConfig.getAuthProtocolConfig().getSsl() != null) {
            properties.put("security.protocol", authConfig.getAuthProtocolConfig().getSsl());
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


    private boolean isTopicExists(TopicConfig topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("connections.max.idle.ms", 5000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = KafkaAdminClient.create(properties)) {
            boolean topicExists = client.listTopics().names().get().stream().anyMatch(topicName -> topicName.equalsIgnoreCase(topic.getName()));
           /* ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            if (!names.isEmpty()) {
                for (String topicName : names) {
                    if (topicName.equalsIgnoreCase(topic.getName()))
                        return true;
                }
            }*/
            return topicExists;
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                LOG.error("Topic does not exist: " + topic.getName());
            }
            throw new RuntimeException("Exception while checking the topics availability...");
        }
    }

    private boolean isKafkaClusterExists(List<String> bootStrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootStrapServers);
        props.put("request.timeout.ms", 10000);
        props.put("connections.max.idle.ms", 5000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean connected = adminClient.describeCluster().clusterId().get() != null;
            return connected;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to connect to the Kafka cluster...");
            throw new RuntimeException(e.getCause());
        }
    }

}
