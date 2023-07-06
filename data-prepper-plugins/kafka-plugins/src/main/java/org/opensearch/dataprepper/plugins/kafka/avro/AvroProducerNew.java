package org.opensearch.dataprepper.plugins.kafka.avro;
import java.util.Properties;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class AvroProducerNew {
    public static void main(String[] args) {
        // Kafka broker properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085");
        props.put("security.protocol", "PLAINTEXT");

        // Create a KafkaProducer instance
        Producer<String, User> producer = new KafkaProducer<>(props);

        // Create a User object
        User user = new User();
        user.setName("John Doe");
        user.setAge("30");
        user.setAddress("123 Main St");

        // Create a ProducerRecord
        String topic = "my-topic";
        String key = "user-key";
        ProducerRecord<String, User> record = new ProducerRecord<>(topic, key, user);

        // Send the record to Kafka
        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                System.out.println("Message sent successfully! Topic: " + metadata.topic() +
                        ", Partition: " + metadata.partition() +
                        ", Offset: " + metadata.offset());
            } else {
                System.err.println("Error sending message: " + exception.getMessage());
            }
        });

        // Close the producer
        producer.close();
    }
}
