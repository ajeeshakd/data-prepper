/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceBufferAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * A helper class which helps to process the Plain Text records and write them
 * into the buffer. Offset handling and consumer re-balancing are also being
 * handled
 */

@SuppressWarnings("deprecation")
public class PlainTextConsumer implements KafkaSourceSchemaConsumer<String, String>, ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(PlainTextConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastReadOffset = 0L;
    private KafkaConsumer<String, String> plainTxtConsumer;
    private volatile long lastCommitTime = System.currentTimeMillis();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void consumeRecords(final KafkaConsumer<String, String> consumer, final AtomicBoolean status,
                               final Buffer<Record<Object>> buffer, final TopicConfig topicConfig, PluginMetrics pluginMetrics, final String schemaType) {
        KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(pluginMetrics, schemaType);
        plainTxtConsumer = consumer;
        try {
            consumer.subscribe(Arrays.asList(topicConfig.getName()));
            while (!status.get()) {
                offsetsToCommit.clear();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty() && records.count() > 0) {
                    for (TopicPartition partition : records.partitions()) {
                        List<Record<Object>> kafkaRecords = new ArrayList<>();
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
                            offsetsToCommit.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                    new OffsetAndMetadata(consumerRecord.offset() + 1, null));
                            kafkaRecords.add(kafkaSourceBufferAccumulator.getEventRecord(consumerRecord.value()));
                            lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        }
                        if (!kafkaRecords.isEmpty()) {
                            kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords, buffer, topicConfig);
                        }
                    }
                    if (!offsetsToCommit.isEmpty() && topicConfig.getConsumerGroupConfig().getAutoCommit().equalsIgnoreCase("false")) {
                        lastCommitTime = kafkaSourceBufferAccumulator.commitOffsets(consumer, lastCommitTime, offsetsToCommit);
                    }
                }
            }
        } catch (Exception exp) {
            LOG.error("Error while reading plain text records from the topic...", exp);
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
        LOG.trace("onPartitionsRevoked() callback triggered and Committing the offsets: {} ", offsetsToCommit);
        try {
            plainTxtConsumer.commitSync(offsetsToCommit);
        } catch (CommitFailedException e) {
            LOG.error("Failed to commit the record...", e);
        }
    }
}
