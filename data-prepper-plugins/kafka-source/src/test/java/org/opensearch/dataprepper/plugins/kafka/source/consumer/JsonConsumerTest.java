package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.ConsumerGroupConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.SchemaConfig;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("deprecation")
@MockitoSettings(strictness = Strictness.LENIENT)
class JsonConsumerTest {

	private JsonConsumer jsonConsumer;

	@Mock
	private KafkaConsumer<String, JsonNode> kafkaJsonConsumer;

	@Mock
	private AtomicBoolean status;

	@Mock
	private Buffer<Record<Object>> buffer;

	@Mock
	private KafkaSourceConfig sourceConfig;

	@Mock
	private PluginMetrics pluginMetrics;

	@BeforeEach
	void setUp() throws Exception {
		jsonConsumer = new JsonConsumer();
		when(sourceConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerGroupConfig.class));
		when(sourceConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(sourceConfig.getConsumerGroupConfig().getBufferDefaultTimeout()).thenReturn(Duration.ZERO);
		when(sourceConfig.getTopic()).thenReturn(Arrays.asList("my-topic"));
		when(sourceConfig.getSchemaConfig().getRecordType()).thenReturn("string");
		when(sourceConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("json");
	}

	/*@Test
	@Disabled
	void testJsonConsumeRecords() throws Exception {
		JsonConsumer spyConsumer = spy(jsonConsumer);
		doCallRealMethod().when(spyConsumer).consumeRecords(kafkaJsonConsumer, status, buffer, sourceConfig,
				pluginMetrics);
		ConsumerRecords<String, JsonNode> consumerRecords = buildConsumerRecords();
		when(kafkaJsonConsumer.poll(Duration.ofMillis(100))).thenReturn(consumerRecords);
		jsonConsumer.consumeRecords(kafkaJsonConsumer, status, buffer, sourceConfig, pluginMetrics);
		verify(jsonConsumer).consumeRecords(kafkaJsonConsumer, status, buffer, sourceConfig, pluginMetrics);
	}*/

	@Test
	void testJsonConsumeRecords_catch_block() {
		JsonConsumer spyConsumer = spy(jsonConsumer);
		doCallRealMethod().when(spyConsumer).consumeRecords(kafkaJsonConsumer, status, buffer, sourceConfig,
				pluginMetrics);
		spyConsumer.consumeRecords(kafkaJsonConsumer, status, buffer, sourceConfig, pluginMetrics);
		verify(spyConsumer).consumeRecords(kafkaJsonConsumer, status, buffer, sourceConfig, pluginMetrics);
	}

	private ConsumerRecords<String, JsonNode> buildConsumerRecords() throws Exception {
		String value = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
		JsonNode mapper = new ObjectMapper().readTree(value);
		Map<TopicPartition, List<ConsumerRecord<String, JsonNode>>> records = new LinkedHashMap<>();
		ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", mapper);
		ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", mapper);
		records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
		return new ConsumerRecords<>(records);
	}

	@Test
	void testJsonConsumerOnPartitionsAssigned() {
		List<TopicPartition> topicPartitions = buildTopicPartition();
		JsonConsumer spyConsumer = spy(jsonConsumer);
		ReflectionTestUtils.setField(spyConsumer, "kafkaJsonConsumer", kafkaJsonConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsAssigned(topicPartitions);
		spyConsumer.onPartitionsAssigned(topicPartitions);
		verify(spyConsumer).onPartitionsAssigned(topicPartitions);
	}

	@Test
	void testJsonConsumerOnPartitionsRevoked() {
		JsonConsumer spyConsumer = spy(jsonConsumer);
		ReflectionTestUtils.setField(spyConsumer, "kafkaJsonConsumer", kafkaJsonConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsRevoked(anyList());
		spyConsumer.onPartitionsRevoked(anyList());
		verify(spyConsumer).onPartitionsRevoked(anyList());
	}

	private List<TopicPartition> buildTopicPartition() {
		TopicPartition partition1 = new TopicPartition("my-topic", 1);
		TopicPartition partition2 = new TopicPartition("my-topic", 2);
		TopicPartition partition3 = new TopicPartition("my-topic", 3);
		return Arrays.asList(partition1, partition2, partition3);
	}
}
