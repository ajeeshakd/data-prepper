package org.opensearch.dataprepper.plugins.kafka.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) {
        // Load the properties file
        Properties props = new Properties();

        /*try {
            props.load(new FileReader("producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        props.put("bootstrap.servers", "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8085");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
        props.put("security.protocol", "PLAINTEXT");


        // Create the producer from the properties
        KafkaProducer<String, Example> producer = new KafkaProducer<>(props);

        // Create some OrderEvents to produce
        ArrayList<Example> examples = new ArrayList<>();
        examples.add(new Example("Black Gloves"));
        examples.add(new Example("Black Hat - New"));
        examples.add(new Example("Gold Hat- New"));
        examples.add(new Example("shoes- New"));
        examples.add(new Example("Trouser- New"));
        examples.add(new Example("T Shirt- New"));
        examples.add(new Example("Casual Shirt- New"));
        examples.add(new Example("Brown Tie"));



        // Turn each OrderEvent into a ProducerRecord for the orders topic, and send them
        for (Example example : examples) {
            /*ProducerRecord<String, OrderEvent> record = new ProducerRecord<>("orders", orderEvent);
            producer.send(record);*/

            final ProducerRecord<String, Example> record = new ProducerRecord("kafka-avro", example);
            producer.send(record);
        }

        // Ensure all messages get sent to Kafka
        producer.flush();
        System.out.println("Done Processing from producer");
    }

}
