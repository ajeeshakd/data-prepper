package org.opensearch.dataprepper.plugins.kafka.avro;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class SampleProducer {
    public SampleProducer() throws IOException {
    }

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

    public static void main(String args[]) throws IOException, InterruptedException {

        final Properties props = loadConfig("D:\\Projects\\kafka-source-demo\\data-prepper\\data-prepper-plugins\\kafka-plugins\\src\\main\\java\\org\\opensearch\\dataprepper\\plugins\\kafka\\avro\\client.properties");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10000; i++) {
            JSONObject json = new JSONObject();
            json.put("name", "Name:" + i);
            producer.send(new ProducerRecord<>("topic-json", json.toString()));
            //producer.send(new ProducerRecord<>("kafka-topic", "key-" + i, ""+i));
            Thread.sleep(200);
            System.out.println("Sent Record : " + json.toString());
        }
        producer.close();
    }
}



