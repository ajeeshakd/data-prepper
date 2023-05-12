/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * * A helper utility class which helps to write different formats of records
 * like json and string to the buffer.
 */
@SuppressWarnings("deprecation")
public class KafkaSourceBufferAccumulator<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBufferAccumulator.class);
  private static final String MESSAGE_KEY = "message";
  private TopicConfig topicConfig;
  private PluginMetrics pluginMetrics;
  private final Counter kafkaConsumerWriteError;
  private static final Long COMMIT_OFFSET_INTERVAL_MILLI_SEC = 300000L;
  private static final String KAFKA_CONSUMER_BUFFER_WRITE_ERROR = "kafkaConsumerBufferWriteError";
  private final JsonFactory jsonFactory = new JsonFactory();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private String schemaType;

  public KafkaSourceBufferAccumulator(TopicConfig topicConfigs, PluginMetrics pluginMetric, String schemaType) {
    this.topicConfig = topicConfigs;
    this.pluginMetrics = pluginMetric;
    this.schemaType = schemaType;
    this.kafkaConsumerWriteError = pluginMetrics.counter(KAFKA_CONSUMER_BUFFER_WRITE_ERROR);
  }

  public Record<Object> getEventRecord(final String line, TopicConfig topicConfig) {Map<String, Object> message = new HashMap<>();
    MessageFormat messageFormat = MessageFormat.getByMessageFormatByName(schemaType);
    if (messageFormat
            .equals(MessageFormat.PLAINTEXT)) {
      message.put(MESSAGE_KEY, line);
    } else if (messageFormat.equals(MessageFormat.JSON) || messageFormat.equals(MessageFormat.AVRO)) {
      try {
        final JsonParser jsonParser = jsonFactory.createParser(line);
        message = objectMapper.readValue(jsonParser, Map.class);
      } catch (Exception e) {
        LOG.error("Unable to parse json data [{}], assuming plain text", line, e);
        message.put(MESSAGE_KEY, line);
      }

    }
    Event event = JacksonLog.builder().withData(message).build();
    return new Record<>(event);
  }

  public synchronized void writeAllRecordToBuffer(List<Record<Object>> kafkaRecords, final Buffer<Record<Object>> buffer, TopicConfig topicConfig) {
    try {
      buffer.writeAll(kafkaRecords,
              topicConfig.getConsumerGroupConfig().getBufferDefaultTimeout().toSecondsPart());
      LOG.info("Total number of records publish in buffer {} for Topic : {}", kafkaRecords.size(),topicConfig.getName());
    } catch (Exception e) {
      LOG.error("Error occurred while writing data to the buffer {}", e.getMessage());
      kafkaConsumerWriteError.increment();
    }
  }

  public long commitOffsets(KafkaConsumer<Object, Object> consumer, long lastCommitTime, Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
    try {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - lastCommitTime > COMMIT_OFFSET_INTERVAL_MILLI_SEC) {
        if(!offsetsToCommit.isEmpty()) {
          consumer.commitSync(offsetsToCommit);
          offsetsToCommit.clear();
          LOG.info("Succeeded to commit the offsets ...");
        }
        lastCommitTime = currentTimeMillis;
      }
    } catch (Exception e) {
      LOG.error("Failed to commit the offsets...", e);
    }
    return lastCommitTime;
  }
}
