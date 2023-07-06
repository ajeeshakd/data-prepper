package org.opensearch.dataprepper.plugins.kafka.json;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class JsonCloudProducer {
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
    public static void main(String[] args) throws InterruptedException, IOException {
        final Properties props = loadConfig("D:\\Projects\\kafka-source-demo\\data-prepper\\data-prepper-plugins\\kafka-plugins\\src\\main\\java\\org\\opensearch\\dataprepper\\plugins\\kafka\\avro\\client.properties");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10000; i++) {
            JSONObject json = new JSONObject();
            json.put("name", "Name:" + i);

            ProducerRecord<String, String> record = new ProducerRecord<>("topic-json", json.toString());

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
