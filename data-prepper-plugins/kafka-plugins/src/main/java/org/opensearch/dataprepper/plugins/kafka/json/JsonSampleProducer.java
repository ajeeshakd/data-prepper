package org.opensearch.dataprepper.plugins.kafka.json;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;

public class JsonSampleProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8085");
        properties.put("security.protocol", "PLAINTEXT");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10000; i++) {
            JSONObject json = new JSONObject();
            json.put("name", "Name:" + i);

            ProducerRecord<String, String> record = new ProducerRecord<>("kafka-json", json.toString());

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("Error producing message: " + exception.getMessage());
                    } else {
                        System.out.println("Message sent successfully! " + json.toString());
                    }
                }
            });
            Thread.sleep(300);
        }
        producer.flush();
        producer.close();
    }
}