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
 * * A helper class which helps to process the Plain Text records and write them
 * into the buffer. Offset handling and consumer re-balancing are also being
 * handled
 */

@SuppressWarnings("deprecation")
public class PlainTextConsumer implements KafkaSourceSchemaConsumer<String, String> {

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void consumeRecords(KafkaConsumer<String, String> consumer, AtomicBoolean status,
                             Buffer<Record<Object>> buffer, TopicConfig topicConfig, PluginMetrics pluginMetrics, String schemaType) {
   //will add the implementation
  }
}
