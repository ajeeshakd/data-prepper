package org.opensearch.dataprepper.plugins.kafka.source.configuration;

import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class SchemaConfigTest {

	@Mock
	SchemaConfig schemaConfig;

	@Mock
	Properties properties;

	@BeforeEach
	void setUp() throws IOException {
		schemaConfig = new SchemaConfig();
		FileReader reader = new FileReader("src\\test\\resources\\pipelines.properties");
		properties = new Properties();
		properties.load(reader);

		schemaConfig.setKeyDeserializer(properties.getProperty("key_deserializer"));
		schemaConfig.setValueDeserializer(properties.getProperty("value_deserializer"));
		schemaConfig.setRecordType(properties.getProperty("record_type"));
		schemaConfig.setSchemaType(properties.getProperty("schema_type"));
		schemaConfig.setRegistryURL(properties.getProperty("registry_url"));
	}

	@Test
	void testConfigValues() {
		assertEquals(schemaConfig.getKeyDeserializer(), properties.getProperty("key_deserializer"));
		assertEquals(schemaConfig.getValueDeserializer(), properties.getProperty("value_deserializer"));
		assertEquals(schemaConfig.getRecordType(), properties.getProperty("record_type"));
		assertEquals(schemaConfig.getSchemaType(), properties.getProperty("schema_type"));
		assertEquals(schemaConfig.getRegistryURL(), properties.getProperty("registry_url"));

	}

}
