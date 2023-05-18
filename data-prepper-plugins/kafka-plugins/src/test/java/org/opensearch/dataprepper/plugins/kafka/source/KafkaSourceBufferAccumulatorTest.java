/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.PlainTextConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doCallRealMethod;

@SuppressWarnings("deprecation")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceBufferAccumulatorTest {

	private PlainTextConsumer textConsumer;
	@Mock
	private KafkaSourceBufferAccumulator<String, String> buffer;

	@Mock
	private KafkaSourceConfig sourceConfig;
	
	@Mock
	private TopicsConfig topicsConfig;
	@Mock
	private KafkaConsumer<Object, Object> kafkaConsumer;
	
	@Mock
	List<TopicsConfig> mockList = new ArrayList<TopicsConfig>();
	@Mock
	private SchemaConfig schemaConfig;
	
	@Mock
	private PluginMetrics pluginMetrics;
	
	@Mock
	private Buffer<Record<Object>> record;

	@Mock
	List<Record<Object>> kafkaRecords;

	@BeforeEach
	void setUp() throws Exception {
		when(sourceConfig.getTopics()).thenReturn((mockList));
		when(mockList.get(0)).thenReturn(topicsConfig);
		when(sourceConfig.getSchemaConfig()).thenReturn(schemaConfig);

		when(sourceConfig.getSchemaConfig()).thenReturn(mock(SchemaConfig.class));

		buffer = new KafkaSourceBufferAccumulator<>(topicsConfig, sourceConfig, "plaintext", pluginMetrics);
	}

	@Test
	void testWriteEventOrStringToBuffer_plaintext_schemaType() throws Exception {
		createObjectWithSchemaType("plaintext"); //Added By Mehak

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord("anyString");
		spyBuffer.getEventRecord("anyString");
		verify(spyBuffer).getEventRecord("anyString");
		assertNotNull(spyBuffer.getEventRecord("anyString"));//Added By Mehak
	}

	@Test
	void testWriteEventOrStringToBuffer_json_schemaType() throws Exception {
		String json = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
		createObjectWithSchemaType("json"); //Added By Mehak

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord(json);
		spyBuffer.getEventRecord(json);
		verify(spyBuffer).getEventRecord(json);
		assertNotNull(spyBuffer.getEventRecord(json));//Added By Mehak
	}

	@Test
	void testWriteEventOrStringToBuffer_json_schemaType_catch_block() throws Exception {
		createObjectWithSchemaType("json"); //Added By Mehak

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord("anyString");
		spyBuffer.getEventRecord("anyString");
		verify(spyBuffer).getEventRecord("anyString");
		assertNotNull(spyBuffer.getEventRecord("anyString"));//Added By Mehak
	}

	@Test
	void testWriteEventOrStringToBuffer_plaintext_schemaType_catch_block() throws Exception {
		createObjectWithSchemaType("plaintext"); //Added By Mehak

		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).getEventRecord(null);
		spyBuffer.getEventRecord(null);
		verify(spyBuffer).getEventRecord(null);
		assertNotNull(spyBuffer.getEventRecord(null)); //Added By Mehak
	}

	@Test
	void testwrite()throws Exception{
		TopicsConfig topicConfig = new TopicsConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		topicConfig.setBufferDefaultTimeout(Duration.ofMillis(100));
		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).write(kafkaRecords, record);
		spyBuffer.write(kafkaRecords, record);
		verify(spyBuffer).write(kafkaRecords, record);
	}

	//Added by Mehak
	private void createObjectWithSchemaType(String schema){

		topicsConfig = new TopicsConfig();
		schemaConfig = new SchemaConfig();
		topicsConfig.setBufferDefaultTimeout(Duration.ofMillis(100));
		sourceConfig.setSchemaConfig(schemaConfig);
		//topicConfig.setConsumerGroupConfig(consumerConfigs);
	}

	@Test
	void testwriteWithBackoff() throws Exception {
		TopicsConfig topicConfig = new TopicsConfig();
		SchemaConfig schemaConfig = new SchemaConfig();
		Buffer<Record<Object>> bufferObj = mock(Buffer.class);
		topicConfig.setBufferDefaultTimeout(Duration.ofMillis(100));
		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).writeWithBackoff(kafkaRecords, bufferObj, topicsConfig);
		spyBuffer.writeWithBackoff(kafkaRecords, bufferObj, topicsConfig);
		verify(spyBuffer).writeWithBackoff(kafkaRecords, bufferObj, topicsConfig);
	}

	@Test
	void testPublishRecordToBuffer_commitOffsets() throws Exception {
		topicsConfig = new TopicsConfig();
		//when(topicsConfig.getAutoCommit()).thenReturn("false");
		KafkaSourceBufferAccumulator<String, String> spyBuffer = spy(buffer);
		doCallRealMethod().when(spyBuffer).commitOffsets(kafkaConsumer, 0L, null);
		spyBuffer.commitOffsets(kafkaConsumer, 0L, null);
		verify(spyBuffer).commitOffsets(kafkaConsumer, 0L, null);
	}
}
