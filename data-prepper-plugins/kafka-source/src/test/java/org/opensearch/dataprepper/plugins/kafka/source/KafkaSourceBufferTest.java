package org.opensearch.dataprepper.plugins.kafka.source;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

@SuppressWarnings("deprecation")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceBufferTest {

	@Mock
	private KafkaSourceBuffer<String, String> buffer;

	@Mock
	private KafkaSourceConfig sourceConfig;
	
	@Mock
	private PluginMetrics pluginMetrics;
	
	@Mock
	private Buffer<Record<Object>> record;
	
	@Mock
	private TopicPartition partition;
	
	@Mock
	private KafkaConsumer<String, ?> consumer; 
	
	@Mock
	private KafkaConsumer<String, String> kafkaconsumer; 
	
	@Mock
	private ConsumerRecord<String, String> consumerRecord;

/*	@BeforeEach
	void setUp() throws Exception {
		when(sourceConfig.getConsumerGroupConfig()).thenReturn(mock(ConsumerGroupConfig.class));
		when(sourceConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));
		
		when(sourceConfig.getSchemaConfig().getRecordType()).thenReturn("string");
		buffer = new KafkaSourceBuffer<>(sourceConfig, pluginMetrics);
	}*/

/*	@Test
	void testWriteEventOrStringToBuffer_plaintext_schemaType() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("plaintext");
		KafkaSourceBuffer<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).writeRecordToBuffer("anyString", record, sourceConfig);
		spyBuffer.writeRecordToBuffer("anyString", record, sourceConfig);
		verify(spyBuffer).writeRecordToBuffer("anyString", record, sourceConfig);
	}

	@Test
	void testWriteEventOrStringToBuffer_json_schemaType() throws Exception {
		String json = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("json");
		KafkaSourceBuffer<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).writeRecordToBuffer(json, record, sourceConfig);
		spyBuffer.writeRecordToBuffer(json, record, sourceConfig);
		verify(spyBuffer).writeRecordToBuffer(json, record, sourceConfig);
	}*/

	/*@Test
	void testWriteEventOrStringToBuffer_avro_schemaType() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("avro");
		KafkaSourceBuffer<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).writeRecordToBuffer("anyString", record, sourceConfig);
		spyBuffer.writeRecordToBuffer("anyString", record, sourceConfig);
		verify(spyBuffer).writeRecordToBuffer("anyString", record, sourceConfig);
	}*/
	
/*	@Test
	void testPublishRecordToBuffer_plaintext_schemaType() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("plaintext");
		KafkaSourceBuffer<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).publishRecordToBuffer("plaintext", record, sourceConfig);
		spyBuffer.publishRecordToBuffer("plaintext", record, sourceConfig);
		verify(spyBuffer).publishRecordToBuffer("plaintext", record, sourceConfig);
	}
	
	@Test
	void testPublishRecordToBuffer_commitOffsets() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("plaintext");
		when(sourceConfig.getConsumerGroupConfig().getAutoCommit()).thenReturn("false");
		KafkaSourceBuffer<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).commitOffsets(partition, 1, kafkaconsumer);
		spyBuffer.commitOffsets(partition, 1, kafkaconsumer);
		verify(spyBuffer).commitOffsets(partition, 1, kafkaconsumer);
	}
	
	@Test
	void testWriteEventOrStringToBuffer_json_schemaType_catch_block() throws Exception {
		when(sourceConfig.getSchemaConfig().getSchemaType()).thenReturn("json");
		KafkaSourceBuffer<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).writeRecordToBuffer("anyString", record, sourceConfig);
		spyBuffer.writeRecordToBuffer("anyString", record, sourceConfig);
		verify(spyBuffer).writeRecordToBuffer("anyString", record, sourceConfig);
	}*/

}
