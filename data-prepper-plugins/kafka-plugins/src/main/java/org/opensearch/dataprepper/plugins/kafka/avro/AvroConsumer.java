package org.opensearch.dataprepper.plugins.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class AvroConsumer {
    public static void main(String[] args) {
        // Load the properties file
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8085");
        props.put("group.id", "avro");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", KafkaAvroDeserializer.class);
        props.put("security.protocol", "PLAINTEXT");

        KafkaConsumer<String, Example> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("kafka-avro"));

        try {
            while (true) {
                ConsumerRecords<String, Example> records = consumer.poll(1000);
                for (ConsumerRecord<String, Example> record : records) {
                    System.out.println(record.value());
                }
            }
        } finally {
            consumer.close();
            System.out.println("Done processing");
        }
    }
}
