/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.micrometer.core.instrument.Counter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * * A helper utility class which helps to write different formats of records
 * like json and string to the buffer.
 */
@SuppressWarnings("deprecation")
public class KafkaSourceBuffer<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBuffer.class);
  private static final String MESSAGE_KEY = "message";
  private KafkaSourceConfig sourceConfig;
  private static final String FALSE = "false";
  private PluginMetrics pluginMetrics;

  private final Counter kafkaConsumerCommitFailed;
  private final Counter kafkaConsumerBufferWriteError;

  private static final String KAFKA_CONSUMER_COMMIT_FAILED = "kafkaConsumerCommitFailed";
  private static final String KAFKA_CONSUMER_BUFFER_WRITE_ERROR = "kafkaConsumerBufferWriteError";

  public KafkaSourceBuffer(KafkaSourceConfig sourceConfig, PluginMetrics pluginMetric) {
    this.sourceConfig = sourceConfig;
    this.pluginMetrics = pluginMetric;
    this.kafkaConsumerCommitFailed = pluginMetrics.counter(KAFKA_CONSUMER_COMMIT_FAILED);
    this.kafkaConsumerBufferWriteError = pluginMetrics.counter(KAFKA_CONSUMER_BUFFER_WRITE_ERROR);
  }

  private Record<Object> getEventRecord(final String line, KafkaSourceConfig sourceConfig) {
    Map<String, Object> plainText = new HashMap<>();

    if (MessageFormat.getByMessageFormatByName(sourceConfig.getSchemaConfig().getSchemaType())
        .equals(MessageFormat.PLAINTEXT)) {
      plainText.put(MESSAGE_KEY, line);
    }
    Event event = JacksonLog.builder().withData(plainText).build();
    return new Record<>(event);

  }

  public void writeRecordToBuffer(String line, final Buffer<Record<Object>> buffer, KafkaSourceConfig sourceConfig) {
    try {
      buffer.write(getEventRecord(line, sourceConfig),
          sourceConfig.getConsumerGroupConfig().getBufferDefaultTimeout().toSecondsPart());
    } catch (TimeoutException e) {
      LOG.error("Timeout while writing plaintext to the buffer {}", e.getMessage());
    }
  }

  public void publishRecordToBuffer(String recordEvent, Buffer<Record<Object>> buffer,
      KafkaSourceConfig sourceConfig) {
    try {
      writeRecordToBuffer(recordEvent, buffer, sourceConfig);
    } catch (Exception exp) {
      LOG.error("Error while writing records to the buffer {}", exp.getMessage());
      kafkaConsumerBufferWriteError.increment();
    }
  }

  public void commitOffsets(TopicPartition partition, long lastOffset, KafkaConsumer<K, V> consumer) {
    try {
      if (FALSE.equalsIgnoreCase(sourceConfig.getConsumerGroupConfig().getAutoCommit())) {
        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));

      }
    } catch (CommitFailedException e) {
      LOG.error("Failed to commit record. Will try again...", e);
      kafkaConsumerCommitFailed.increment();
    }
  }
}
