/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * An interface with a generic method which helps to process the records for
 * avro or json or plain text consumer dynamically.
 */
@SuppressWarnings("deprecation")
public interface KafkaSourceSchemaConsumer<K, V> {

  public void consumeRecords(final KafkaConsumer<K, V> consumer, AtomicBoolean status,
                             Buffer<Record<Object>> buffer, final TopicConfig sourceConfig, PluginMetrics pluginMetrics, final String schemaType);
}
