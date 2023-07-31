package schema.registry.local;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class AvroProducer {

    public static void main(String[] args) throws InterruptedException {
        // Load the properties file
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8085");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
       // props.put("security.protocol", "PLAINTEXT");

        // Create the producer from the properties
        KafkaProducer<String, Example> producer = new KafkaProducer<>(props);

        String str;
        for (int i=0;i<50000;i++) {
            str = getRandomString();
            Example example = new Example("AVRO-"+str);
            final ProducerRecord<String, Example> record = new ProducerRecord("kafka-avro-test", example);
            producer.send(record);
            Thread.sleep(100);
            System.out.println("Produced Message: "+str);
        }

        // Ensure all messages get sent to Kafka
        producer.flush();
        System.out.println("Done Processing from producer");
    }
    static String getRandomString(){
        String upperAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerAlphabet = "abcdefghijklmnopqrstuvwxyz";
        String numbers = "0123456789";

        // combine all strings
        String alphaNumeric = upperAlphabet + lowerAlphabet + numbers;

        // create random string builder
        StringBuilder sb = new StringBuilder();

        // create an object of Random class
        Random random = new Random();

        // specify length of random string
        int length = 10;

        for(int i = 0; i < length; i++) {

            // generate random index number
            int index = random.nextInt(alphaNumeric.length());

            // get character specified by index
            // from the string
            char randomChar = alphaNumeric.charAt(index);

            // append the character to string builder
            sb.append(randomChar);
        }
        return sb.toString();
        /*String randomString = sb.toString();
        System.out.println("Random String is: " + randomString);*/

    }

}
