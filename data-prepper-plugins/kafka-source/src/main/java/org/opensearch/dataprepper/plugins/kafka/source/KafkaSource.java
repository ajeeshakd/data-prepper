/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.source.consumer.MultithreadedConsumer;
import org.opensearch.dataprepper.plugins.kafka.source.deserializer.KafkaSourceJsonDeserializer;
import org.opensearch.dataprepper.plugins.kafka.source.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
  private final KafkaSourceConfig sourceConfig;
  private final List<MultithreadedConsumer> consumers = new ArrayList<>();
  private ExecutorService executorService;
  private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
  private final Counter kafkaWorkerThreadProcessingErrors;
  private final PluginMetrics pluginMetrics;

  @DataPrepperPluginConstructor
  public KafkaSource(final KafkaSourceConfig sourceConfig, final PluginMetrics pluginMetrics) {
    this.sourceConfig = sourceConfig;
    this.pluginMetrics = pluginMetrics;
    this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
  }

  @Override
  public void start(Buffer<Record<Object>> buffer) {
    int consumerCount = sourceConfig.getConsumerGroupConfig().getWorkers();
    String consumerGroups = sourceConfig.getConsumerGroupConfig().getGroupName();
    Properties consumerProperties = getConsumerProperties();
    if (!sourceConfig.getTopic().isEmpty() && isTopicAvailable(sourceConfig.getTopic(), consumerProperties)) {
      try {
        if (executorService == null || executorService.isShutdown()) {
          executorService = Executors.newFixedThreadPool(consumerCount);
          IntStream.range(0, consumerCount).forEach(index -> {
            String consumerId = Integer.toString(index + 1);
            MultithreadedConsumer multithreadedConsumer = new MultithreadedConsumer(consumerId,
                consumerGroups, consumerProperties, sourceConfig, buffer, pluginMetrics);
            consumers.add(multithreadedConsumer);
            executorService.submit(multithreadedConsumer);
          });
        }
      } catch (Exception e) {
        LOG.error("Failed to setup Kafka source.", e);
        kafkaWorkerThreadProcessingErrors.increment();
      }
    } else {
      LOG.info("The given TOPIC : {} doesn't exists. No records read.", sourceConfig.getTopic());
    }
  }

  @Override
  public void stop() {
    LOG.info("Shutting down Consumers...");
    for (MultithreadedConsumer kafkaConsumer : consumers) {
      kafkaConsumer.shutdownConsumer();
    }
    LOG.info("Closing the Kafka Consumer Application...");
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(
          sourceConfig.getConsumerGroupConfig().getThreadWaitingTime().toSeconds(), TimeUnit.MILLISECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      if (e.getCause() instanceof InterruptedException) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  private Properties getConsumerProperties() {
    Properties properties = new Properties();
    try {
      properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
          sourceConfig.getConsumerGroupConfig().getAutoCommitInterval().toSecondsPart());
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          sourceConfig.getConsumerGroupConfig().getAutoOffsetReset());
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
          sourceConfig.getConsumerGroupConfig().getAutoCommit());
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, sourceConfig.getConsumerGroupConfig().getGroupId());

      setPropertiesForSchemaType(properties, sourceConfig.getSchemaConfig().getSchemaType());
    } catch (Exception e) {
      LOG.error("Unable to create Kafka consumer from the given configuration.", e);
    }

    return properties;
  }

  private void setPropertiesForSchemaType(Properties properties, String schemaType) {

    if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          sourceConfig.getSchemaConfig().getKeyDeserializer());
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaSourceJsonDeserializer.class);
    } else if (schemaType.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          sourceConfig.getSchemaConfig().getKeyDeserializer());
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          sourceConfig.getSchemaConfig().getValueDeserializer());
    }
  }

  private boolean isTopicAvailable(List<String> topicList, Properties properties) {
    AdminClient adminClient = AdminClient.create(properties);
    boolean isTopicExist = false;
    try {
      for (String topic : topicList) {
        isTopicExist = adminClient.listTopics().names().get().stream()
            .anyMatch(topicName -> topicName.equalsIgnoreCase(topic));
        if (!isTopicExist) {
          return isTopicExist;
        }
      }
    } catch (Exception e) {
      LOG.error("Error while checking the given topic name : {}", e.getMessage());
      if (e.getCause() instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    } finally {
      adminClient.close();
    }
    return isTopicExist;
  }

}
