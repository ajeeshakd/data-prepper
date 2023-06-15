package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;

class KafkaSourceCustomConsumerTest {

    private KafkaSourceCustomConsumer consumer;

    @Mock
    private KafkaConsumer<String, Object> plainTextConsumer;

    @Mock
    private KafkaConsumer<String, Object> jsonConsumer;

    @Mock
    private KafkaConsumer<String, Object> avroConsumer;


    private AtomicBoolean status;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private TopicConfig topicConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    private String schemaType = null;


    @BeforeEach
    void setUp() throws Exception {
        //  plainTextConsumer = new PlainTextConsumer();
        //Added to load Yaml file - Start
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

        plainTextConsumer = mock(KafkaConsumer.class);
        jsonConsumer = mock(KafkaConsumer.class);
        avroConsumer = mock(KafkaConsumer.class);

        status = new AtomicBoolean();
        //System.setProperty("atomic_value", "true");
    }

    @Test
    void testPlainTextConsumeRecords() throws Exception {
        /*schemaType = "plaintext";
        Properties prop = new Properties();
        consumer = new KafkaSourceCustomConsumer(plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
       // KafkaSourceCustomConsumer custConsumer = new KafkaSourceCustomConsumer(null,null,null,null,null,null,null);
        //ReflectivelySetField.setField(KafkaSourceCustomConsumer.class, consumer, "status", true);
        when(plainTextConsumer.poll(Duration.ofMillis(1))).thenReturn(buildConsumerRecords());

        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        doCallRealMethod().when(spyConsumer).consumeRecords();
        spyConsumer.consumeRecords();
        verify(spyConsumer).consumeRecords();*/
    }

    @Test
    void testPlaintextConsumer(){
        /*KafkaSourceCustomConsumer kafkaConsumerLocal = mock(KafkaSourceCustomConsumer.class);
        KafkaConsumer kafkaConsumer = spy(new KafkaConsumer("topic-name"));

        ReflectionTestUtils.setField(kafkaConsumer, "threadPoolCount", 1);
        ReflectionTestUtils.setField(kafkaConsumer, "consumer", kafkaConsumerLocal);

       // doNothing().when(kafkaConsumer).runConsumer();
       // doNothing().when(kafkaConsumer).addShutDownHook();
        doReturn(kafkaConsumerLocal).when(consumerInit).getKafkaConsumer();*/
    }

    @Test
    void jsonConsumeRecords() throws Exception {
       /* schemaType = "json";
        when(plainTextConsumer.poll(Duration.ofMillis(100))).thenReturn(buildConsumerRecords());
        consumer = new KafkaSourceCustomConsumer(jsonConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        doCallRealMethod().when(spyConsumer).consumeRecords();
        spyConsumer.consumeRecords();
        verify(spyConsumer).consumeRecords();*/
    }

    @Test
    void avroConsumeRecords() throws Exception {
       /* schemaType = "avro";
        when(plainTextConsumer.poll(Duration.ofMillis(100))).thenReturn(buildConsumerRecords());
        consumer = new KafkaSourceCustomConsumer(avroConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        doCallRealMethod().when(spyConsumer).consumeRecords();
        spyConsumer.consumeRecords();
        verify(spyConsumer).consumeRecords();*/
    }


    private ConsumerRecords<String, Object> buildConsumerRecords() throws Exception {
        String value = null;
        Map<TopicPartition, List<ConsumerRecord<String, Object>>> records = new LinkedHashMap<>();
        if (schemaType != null && schemaType.equalsIgnoreCase("plaintext")) {
            value = "test message";

            ConsumerRecord<String, Object> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", "myvalue");
            ConsumerRecord<String, Object> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", "myvalue");
            records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
            return new ConsumerRecords(records);
        } else {
            value = "{\"writebuffer\":\"true\",\"buffertype\":\"json\"}";
            JsonNode mapper = new ObjectMapper().readTree(value);
            ConsumerRecord<String, Object> record1 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", mapper);
            ConsumerRecord<String, Object> record2 = new ConsumerRecord<>("my-topic", 0, 0L, "mykey", mapper);
            records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));
            return new ConsumerRecords<>(records);
        }


    }

    @Test
    void testJsonConsumerOnPartitionsRevoked() {
        consumer = new KafkaSourceCustomConsumer(plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics);
        KafkaSourceCustomConsumer spyConsumer = spy(consumer);
        ReflectionTestUtils.setField(spyConsumer, "kafkaConsumer", plainTextConsumer);
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