/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {
    @Mock
    private KafkaSource source;

    @Mock
    private KafkaSourceConfig kafkaSourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExecutorService executorService;

    @Mock
    private TopicsConfig topicsConfig;

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @BeforeEach
    void setUp() throws Exception {
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        if(data instanceof Map){
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
        }
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
        source = new KafkaSource(kafkaSourceConfig, pluginMetrics);
        KafkaSource spySource = spy(source);
        ReflectionTestUtils.setField(spySource, "executorService", executorService);
        doCallRealMethod().when(spySource).stop();
        spySource.stop();
        verify(spySource).stop();
    }

    private List<MultithreadedConsumer> buildKafkaSourceConsumer() {
        List<MultithreadedConsumer> consumers = new ArrayList<>();
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(
                topicsConfig.getGroupId(),
                topicsConfig.getGroupId(),
                prop, kafkaSourceConfig, null, pluginMetrics);
        consumers.add(kafkaSourceConsumer);
        return consumers;
    }


    @Test
    void test_kafkaSource_start_execution_string_schemaType() throws Exception {

        source = new KafkaSource(kafkaSourceConfig, pluginMetrics);
        KafkaSource spySource = spy(source);
        doCallRealMethod().when(spySource).start(any());
        spySource.start(any());
        verify(spySource).start(any());
    }

    @Test
    void test_kafkaSource_start_execution_json_schemaType() throws Exception {

        source = new KafkaSource(kafkaSourceConfig, pluginMetrics);
        KafkaSource spySource = spy(source);
        ReflectionTestUtils.setField(spySource, "sourceConfig", kafkaSourceConfig);
        doCallRealMethod().when(spySource).start(any());
        spySource.start(any());
        verify(spySource).start(any());
    }
}
