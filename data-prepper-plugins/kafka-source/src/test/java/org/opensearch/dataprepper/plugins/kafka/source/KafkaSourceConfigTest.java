package org.opensearch.dataprepper.plugins.kafka.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.ConsumerGroupConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.SchemaConfig;

class KafkaSourceConfigTest {

	@Mock
	KafkaSourceConfig kafkaSourceConfig;

	@Mock
	Properties properties;

	@Mock
	ConsumerGroupConfig consumerGroupConfig;

	@Mock
	SchemaConfig schemaConfig;

	@BeforeEach
	void setUp() throws IOException {
		kafkaSourceConfig = new KafkaSourceConfig();
		consumerGroupConfig = new ConsumerGroupConfig();
		schemaConfig = new SchemaConfig();
		FileReader reader = new FileReader("src\\test\\resources\\pipelines.properties");
		properties = new Properties();
		properties.load(reader);

		consumerGroupConfig.setAutoCommit("false");
		consumerGroupConfig.setGroupId("Kafka-1");
		schemaConfig.setRegistryURL("localhost:8082");

		kafkaSourceConfig.setConsumerGroupConfig(consumerGroupConfig);
		kafkaSourceConfig.setSchemaConfig(schemaConfig);
		kafkaSourceConfig.setBootStrapServers(Arrays.asList(properties.getProperty("bootstrap_servers")));
		kafkaSourceConfig.setTopic(Arrays.asList(properties.getProperty("topic")));
	}

	@Test
	void testConfigValues() {
		assertEquals(kafkaSourceConfig.getBootStrapServers(),
				Arrays.asList(properties.getProperty("bootstrap_servers")));
		assertEquals(kafkaSourceConfig.getTopic(), Arrays.asList(properties.getProperty("topic")));
		assertNotNull(kafkaSourceConfig.getConsumerGroupConfig());
		assertNotNull(kafkaSourceConfig.getSchemaConfig());

	}

}
