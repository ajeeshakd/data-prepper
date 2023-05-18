package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
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
import org.opensearch.dataprepper.plugins.kafka.configuration.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("deprecation")
@MockitoSettings(strictness = Strictness.LENIENT)
class AvroConsumerTest {

	private AvroConsumer avroConsumer;

	@Mock
	private KafkaConsumer<String, GenericRecord> kafkaAvroConsumer;

	@Mock
	private AtomicBoolean status;

	@Mock
	private Buffer<Record<Object>> buffer;

	@Mock
	private KafkaSourceConfig sourceConfig;
	
	@Mock
	private TopicsConfig topicsConfig;
	
	@Mock
	List<TopicsConfig> mockList = new ArrayList<TopicsConfig>();


	@Mock
	private PluginMetrics pluginMetrics;

	@Mock
	private SchemaConfig schemaConfig;

	@BeforeEach
	void setUp() throws Exception {
		avroConsumer = new AvroConsumer();
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		/*when(topicsConfig.getTopic()).thenReturn(topicConfig);
		when(topicConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerConfigs.class));
		
		when(topicConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		when(topicConfig.getConsumerGroupConfig().getBufferDefaultTimeout()).thenReturn(Duration.ZERO);
		when(topicConfig.getName()).thenReturn(("my-topic"));
		when(topicConfig.getSchemaConfig().getRecordType()).thenReturn("string");
		when(topicConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		when(topicConfig.getSchemaConfig().getSchemaType()).thenReturn("json");*/
	}

	@Test
	void testJsonConsumeRecords_catch_block() {
		AvroConsumer spyConsumer = spy(avroConsumer);
		doCallRealMethod().when(spyConsumer).consumeRecords(kafkaAvroConsumer, status, buffer, topicsConfig,
				sourceConfig, pluginMetrics, "avro");
		spyConsumer.consumeRecords(kafkaAvroConsumer, status, buffer, topicsConfig, sourceConfig, pluginMetrics, "avro");
		verify(spyConsumer).consumeRecords(kafkaAvroConsumer, status, buffer, topicsConfig, sourceConfig, pluginMetrics, "avro");
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
		AvroConsumer spyConsumer = spy(avroConsumer);
		ReflectionTestUtils.setField(spyConsumer, "kafkaAvroConsumer", kafkaAvroConsumer);
		doCallRealMethod().when(spyConsumer).onPartitionsAssigned(topicPartitions);
		spyConsumer.onPartitionsAssigned(topicPartitions);
		verify(spyConsumer).onPartitionsAssigned(topicPartitions);
	}

//	@Test
//	void testJsonConsumerOnPartitionsRevoked() {
//		AvroConsumer spyConsumer = spy(avroConsumer);
//		ReflectionTestUtils.setField(spyConsumer, "kafkaAvroConsumer", kafkaAvroConsumer);
//		doCallRealMethod().when(spyConsumer).onPartitionsRevoked(anyList());
//		spyConsumer.onPartitionsRevoked(anyList());
//		verify(spyConsumer).onPartitionsRevoked(anyList());
//	}


	private List<TopicPartition> buildTopicPartition() {
		TopicPartition partition1 = new TopicPartition("my-topic", 1);
		TopicPartition partition2 = new TopicPartition("my-topic", 2);
		TopicPartition partition3 = new TopicPartition("my-topic", 3);
		return Arrays.asList(partition1, partition2, partition3);
	}
/*
	@Test
	void testJsonConsumer_write_to_buffer_positive_case() throws Exception{
	JsonConsumer spyConsumer = spy(jsonConsumer);
	String value = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
	JsonNode mapper = new ObjectMapper().readTree(value);
	System.out.println("mapper::"+mapper);
	spyConsumer.writeToBuffer(mapper, buffer);
	verify(spyConsumer).writeToBuffer(mapper, buffer);
	}

	@Test
	void testJsonConsumer_write_to_buffer_exception_case() throws Exception{
	JsonConsumer spyConsumer = spy(jsonConsumer);
	assertThrows(Exception.class, () ->
	spyConsumer.writeToBuffer(new ObjectMapper().readTree("test"), buffer));
	}*/
}
