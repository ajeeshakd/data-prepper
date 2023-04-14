/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
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
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceBuffer;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * A helper class which helps to process the JSON records and write them into
 * the buffer. Offset handling and consumer re balance are also being handled
 */

@SuppressWarnings("deprecation")
public class JsonConsumer implements KafkaSchemaTypeConsumer<String, JsonNode>, ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(JsonConsumer.class);
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  private KafkaConsumer<String, JsonNode> kafkaJsonConsumer;
  private long lastReadOffset = 0L;
  private final JsonFactory jsonFactory = new JsonFactory();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String MESSAGE_KEY = "message";
	
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void consumeRecords(KafkaConsumer<String, JsonNode> consumer, AtomicBoolean status,
      Buffer<Record<Object>> buffer, TopicConfig topicConfig, PluginMetrics pluginMetrics) {
    KafkaSourceBuffer kafkaSourceBuffer = new KafkaSourceBuffer(topicConfig, pluginMetrics);
    kafkaJsonConsumer = consumer;
    try {
      currentOffsets.clear();
      consumer.subscribe(Arrays.asList(topicConfig.getName()));
      while (!status.get()) {
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
        if (!records.isEmpty() && records.count() > 0) {
          for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, JsonNode>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, JsonNode> consumerRecord : partitionRecords) {
              currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1, null));
              writeToBuffer(consumerRecord.value(), buffer);	
              lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
              kafkaSourceBuffer.commitOffsets(partition, lastReadOffset, consumer);
            }
          }
        }
      }
    } catch (Exception exp) {
      LOG.error("Error while reading the json records from the topic...{}", exp.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  public void writeToBuffer(final JsonNode jsonNode, Buffer<Record<Object>> buffer) throws TimeoutException {
    try {
      final JsonParser jsonParser = jsonFactory.createParser(jsonNode.toString());
      final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);
      Event event = JacksonLog.builder().withData(innerJson).build();
      Record<Object> jsonRecord = new Record<>(event);
      buffer.write(jsonRecord, 1200);
    } catch (Exception e) {
      LOG.error("Unable to parse json data [{}], assuming plain text", jsonNode, e);
      final Map<String, Object> plainMap = new HashMap<>();
      plainMap.put(MESSAGE_KEY, jsonNode.toString());
      Event event = JacksonLog.builder().withData(plainMap).build();
      Record<Object> jsonRecord = new Record<>(event);
      buffer.write(jsonRecord, 1200);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    LOG.trace("onPartitionsAssigned() callback triggered and Closing the Json consumer...");
    for (TopicPartition partition : partitions) {
      kafkaJsonConsumer.seek(partition, lastReadOffset);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    LOG.trace("onPartitionsRevoked() callback triggered and Committing the offsets for Json consumer: {} ",
        currentOffsets);
    try {
      kafkaJsonConsumer.commitSync(currentOffsets);
    } catch (CommitFailedException e) {
      LOG.error("Failed to commit the record for the Json consumer...", e);
    }
  }

}
