/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.ConsumerGroupConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.source.consumer.MultithreadedConsumer;
import org.opensearch.dataprepper.plugins.kafka.source.deserializer.KafkaSourceJsonDeserializer;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {

	@Mock
	private KafkaSource source;

	@Mock
	private KafkaSourceConfig sourceConfig;

	@Mock
	private PluginMetrics pluginMetrics;

	@Mock
	private ExecutorService executorService;

	@BeforeEach
	void setUp() throws Exception {
		when(sourceConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerGroupConfig.class));
		when(sourceConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(sourceConfig.getConsumerGroupConfig().getWorkers()).thenReturn(1);
		when(sourceConfig.getConsumerGroupConfig().getGroupName()).thenReturn("DPKafkaProj");
		when(sourceConfig.getConsumerGroupConfig().getAutoOffsetReset()).thenReturn("earliest");
		when(sourceConfig.getBootStrapServers()).thenReturn(Arrays.asList("localhost:9092"));
		when(sourceConfig.getConsumerGroupConfig().getGroupId()).thenReturn("DPKafkaProj");
		when(sourceConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		when(sourceConfig.getTopic()).thenReturn(Arrays.asList("my-topic"));

	}

	@Test
	void test_kafkaSource_start_execution_string_schemaType() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("plaintext");
		when(sourceConfig.getSchemaConfig().getValueDeserializer()).thenReturn(StringDeserializer.class.getName());
		source = new KafkaSource(sourceConfig, pluginMetrics);
		KafkaSource spySource = spy(source);
		doCallRealMethod().when(spySource).start(any());
		spySource.start(any());
		verify(spySource).start(any());
	}

	@Test
	void test_kafkaSource_start_execution_json_schemaType() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("json");
		when(sourceConfig.getSchemaConfig().getKeyDeserializer())
				.thenReturn(KafkaSourceJsonDeserializer.class.getName());
		source = new KafkaSource(sourceConfig, pluginMetrics);
		KafkaSource spySource = spy(source);
		doCallRealMethod().when(spySource).start(any());
		spySource.start(any());
		verify(spySource).start(any());
	}

	@Test
	void test_kafkaSource_start_execution_catch_block() {
		source = new KafkaSource(null, pluginMetrics);
		KafkaSource spySource = spy(source);
		Assertions.assertThrows(Exception.class, () -> spySource.start(any()));
	}

	@Test
	void test_kafkaSource_stop_execution() throws Exception {
		List<MultithreadedConsumer> consumers = buildKafkaSourceConsumer();
		source = new KafkaSource(sourceConfig, pluginMetrics);
		KafkaSource spySource = spy(source);
		ReflectionTestUtils.setField(spySource, "executorService", executorService);
		ReflectionTestUtils.setField(spySource, "consumers", consumers);
		doCallRealMethod().when(spySource).stop();
		spySource.stop();
		verify(spySource).stop();
	}

	private List<MultithreadedConsumer> buildKafkaSourceConsumer() {
		List<MultithreadedConsumer> consumers = new ArrayList<>();
		Properties prop = new Properties();
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(
				sourceConfig.getConsumerGroupConfig().getGroupId(), sourceConfig.getConsumerGroupConfig().getGroupId(),
				prop, sourceConfig, null, pluginMetrics);
		consumers.add(kafkaSourceConsumer);
		return consumers;
	}
}
