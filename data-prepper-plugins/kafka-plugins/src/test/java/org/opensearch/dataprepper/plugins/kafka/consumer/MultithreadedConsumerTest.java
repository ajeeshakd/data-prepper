package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;
import java.util.Map;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doCallRealMethod;


class MultithreadedConsumerTest {
    @Mock
    Properties properties;
    @Mock
    KafkaSourceConfig sourceConfig;
    @Mock
    TopicConfig topicConfig;
    @Mock
    Buffer<Record<Object>> buffer;
    @Mock
    PluginMetrics pluginMetrics;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @BeforeEach
    void setUp() throws IOException {
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            sourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            topicConfig = sourceConfig.getTopics().get(0);
        }
        pluginMetrics = mock(PluginMetrics.class);
        buffer = mock(Buffer.class);
    }

    private MultithreadedConsumer createObjectUnderTest(String consumerId,
                                                        String consumerGroupId,
                                                        String schema) {
        return new MultithreadedConsumer(consumerId,
                consumerGroupId,
                properties,
                topicConfig,
                sourceConfig,
                buffer,
                pluginMetrics,
                schema);
    }

    @Test
    void testRunWithPlainText() {
        Properties prop = getProperties();
        MultithreadedConsumer multithreadedConsumer = new MultithreadedConsumer(
                topicConfig.getGroupId(),
                topicConfig.getGroupId(),
                prop, topicConfig, sourceConfig, buffer, pluginMetrics, "plaintext");
        MultithreadedConsumer spySource = spy(multithreadedConsumer);
        doCallRealMethod().when(spySource).run();
        spySource.run();
        verify(spySource).run();
    }

    @Test
    void testRunWithJson() {
        Properties prop = getProperties();
        MultithreadedConsumer multithreadedConsumer = new MultithreadedConsumer(
                topicConfig.getGroupId(),
                topicConfig.getGroupId(),
                prop, topicConfig, sourceConfig, buffer, pluginMetrics, "json");
        MultithreadedConsumer spySource = spy(multithreadedConsumer);
        doCallRealMethod().when(spySource).run();
        spySource.run();
        verify(spySource).run();
    }

    @Test
    void testRunWithAvro() {
        Properties prop = getProperties();
        MultithreadedConsumer multithreadedConsumer = new MultithreadedConsumer(
                topicConfig.getGroupId(),
                topicConfig.getGroupId(),
                prop, topicConfig, sourceConfig, buffer, pluginMetrics, "avro");
        MultithreadedConsumer spySource = spy(multithreadedConsumer);
        doCallRealMethod().when(spySource).run();
        spySource.run();
        verify(spySource).run();
    }

    @Test
    void testShutdownConsumer() {
        Properties prop = getProperties();
        MultithreadedConsumer multithreadedConsumer = new MultithreadedConsumer(
                topicConfig.getGroupId(),
                topicConfig.getGroupId(),
                prop, topicConfig, sourceConfig, buffer, pluginMetrics, "avro");
        MultithreadedConsumer spySource = spy(multithreadedConsumer);
        doCallRealMethod().when(spySource).shutdownConsumer();
        spySource.shutdownConsumer();
        verify(spySource).shutdownConsumer();
    }

    private Properties getProperties() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return prop;
    }

}