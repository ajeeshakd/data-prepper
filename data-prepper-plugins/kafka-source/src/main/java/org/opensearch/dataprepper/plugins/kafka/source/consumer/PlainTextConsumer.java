/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceBuffer;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * A helper class which helps to process the Plain Text records and write them
 * into the buffer. Offset handling and consumer re balance are also being
 * handled
 */

@SuppressWarnings("deprecation")
public class PlainTextConsumer implements KafkaSchemaTypeConsumer<String, String>, ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(PlainTextConsumer.class);
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  private long lastReadOffset = 0L;
  private KafkaConsumer<String, String> plainTxtConsumer;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void consumeRecords(KafkaConsumer<String, String> consumer, AtomicBoolean status,
      Buffer<Record<Object>> buffer, KafkaSourceConfig sourceConfig, PluginMetrics pluginMetrics) {
    KafkaSourceBuffer kafkaSourceBuffer = new KafkaSourceBuffer(sourceConfig, pluginMetrics);
    plainTxtConsumer = consumer;
    try {
      currentOffsets.clear();
      consumer.subscribe(sourceConfig.getTopic());
      while (!status.get()) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(sourceConfig.getConsumerGroupConfig().getMaxPollInterval().toSecondsPart()));
        if (!records.isEmpty() && records.count() > 0) {
          for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
              currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                 new OffsetAndMetadata(consumerRecord.offset() + 1, null));
              kafkaSourceBuffer.publishRecordToBuffer(consumerRecord.value(), buffer, sourceConfig);
              lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
              kafkaSourceBuffer.commitOffsets(partition, lastReadOffset, consumer);
            }
          }
        }
      }
    } catch (Exception exp) {
      LOG.error("Error while reading plain text records from the topic...{}", exp.getMessage());
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    LOG.trace("onPartitionsAssigned() callback triggered and Closing the consumer...");
    for (TopicPartition partition : partitions) {
      plainTxtConsumer.seek(partition, lastReadOffset);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    LOG.trace("onPartitionsRevoked() callback triggered and Committing the offsets: {} ", currentOffsets);
    try {
      plainTxtConsumer.commitSync(currentOffsets);
    } catch (CommitFailedException e) {
      LOG.error("Failed to commit the record...", e);
    }
  }
}
