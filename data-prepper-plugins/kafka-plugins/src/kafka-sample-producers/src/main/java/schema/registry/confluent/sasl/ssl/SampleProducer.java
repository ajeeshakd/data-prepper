package schema.registry.confluent.sasl.ssl;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

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

    public static void main(String args[]) throws IOException, InterruptedException, JSONException {
        String basePath = new File("").getAbsolutePath();
        final Properties props = loadConfig(basePath.concat("\\src\\main\\java\\schema\\registry\\confluent\\sasl\\ssl\\client.properties"));
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10000; i++) {
            JSONObject json = new JSONObject();
            json.put("name", getRandomString());
            producer.send(new ProducerRecord<>("topic-json", json.toString()));
            //producer.send(new ProducerRecord<>("kafka-topic", "key-" + i, ""+i));
            Thread.sleep(200);
            System.out.println("Sent Record : " + json.toString());
        }
        producer.close();
    }
    static String getRandomString(){
        String upperAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerAlphabet = "abcdefghijklmnopqrstuvwxyz";
        String numbers = "0123456789";

        String alphaNumeric = upperAlphabet + lowerAlphabet + numbers;
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        int length = 10;

        for(int i = 0; i < length; i++) {
            int index = random.nextInt(alphaNumeric.length());
            char randomChar = alphaNumeric.charAt(index);
            sb.append(randomChar);
        }
        return sb.toString();
    }
}



