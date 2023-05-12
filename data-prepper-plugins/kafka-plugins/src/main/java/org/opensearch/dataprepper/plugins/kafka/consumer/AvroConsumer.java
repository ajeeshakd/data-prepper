/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;


import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceBufferAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeoutException;


/**
 * * A helper class which helps to process the avro records and write them into the buffer.
 * Offset handling and consumer re-balance are also being handled
 */

@SuppressWarnings("deprecation")
public class AvroConsumer implements KafkaSourceSchemaConsumer<String, GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastReadOffset = 0L;
    private KafkaConsumer<String, GenericRecord> kafkaAvroConsumer;
    private volatile long lastCommitTime = System.currentTimeMillis();
    private static final Long COMMIT_OFFSET_INTERVAL_MILLI_SEC = 300000L;

    @Override
    public void consumeRecords(KafkaConsumer<String, GenericRecord> consumer, AtomicBoolean status, Buffer<Record<Object>> buffer, TopicConfig topicConfig, PluginMetrics pluginMetrics, String schemaType) {
        KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(topicConfig, pluginMetrics, schemaType);
        kafkaAvroConsumer = consumer;
        System.out.println("inside avro consumer");
        try{
            System.out.println("topicConfig.getName()::"+topicConfig.getName());
            consumer.subscribe(Arrays.asList(topicConfig.getName()));
            while (!status.get()) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty() && records.count() > 0) {
                    for (TopicPartition partition : records.partitions()) {
                        List<Record<Object>> kafkaRecords = new ArrayList<>();
                        List<ConsumerRecord<String, GenericRecord>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, GenericRecord> consumerRecord : partitionRecords) {
                            offsetsToCommit.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                    new OffsetAndMetadata(consumerRecord.offset() + 1, null));
                            System.out.println("consumerRecord.value()::"+consumerRecord.value());
                            kafkaRecords.add(kafkaSourceBufferAccumulator.getEventRecord(consumerRecord.value().toString(),topicConfig));
                            lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        }
                        if (!kafkaRecords.isEmpty()){
                            kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords,buffer,topicConfig);
                        }
                    }
                    if(!offsetsToCommit.isEmpty()) {
                        commitOffsets(consumer);
                    }
                }
            }
        }
        catch (Exception exp){
            LOG.error("Error while reading avro records from the topic...{}", exp.getMessage());
            exp.printStackTrace();
        }
    }

    public void commitOffsets(KafkaConsumer<String, GenericRecord> consumer) {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > COMMIT_OFFSET_INTERVAL_MILLI_SEC) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                    LOG.error("Succeeded to commit the offsets ...");
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            LOG.error("Failed to commit the offsets...", e);
        }
    }

    private void writeToBuffer(final GenericRecord record, Buffer<Record<Object>> buffer) throws TimeoutException {
        try {
           // System.out.println("key::"+record.ge);
            buffer.write((Record<Object>) record, 1200);
        } catch (Exception e) {
           //   System.out.println("Exception occured"+e);
           // LOG.error("Unable to parse json data [{}], assuming plain text", jsonNode, e);
//            final Map<String, Object> plainMap = new HashMap<>();
//            plainMap.put(MESSAGE_KEY, jsonNode.toString());
//            Event event = JacksonLog.builder().withData(plainMap).build();
//            Record<Object> jsonRecord = new Record<>(event);
//            buffer.write(jsonRecord, 1200);
        }
    }
}
