package schema.registry.local;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class JsonMockProducer {

    public static void main(String args[]){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        properties.put("schema.registry.url", "http://localhost:8085");
        //properties.put("security.protocol", "PLAINTEXT");
        //properties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        KafkaProducer<String, SampleRec> producer = new KafkaProducer<>(properties);
        //String topic = "json";
        new Thread(() -> {
            IntStream.range(1, 100000).forEach(i -> {
                System.out.println("producing "+ i);
                producer.send(new ProducerRecord<String, SampleRec>
                                ("kafka-json",
                                        "key" + i,
                                        new SampleRec(getRandomString()))
                        , new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if(exception!=null){
                                    exception.printStackTrace();
                                }else{
                                    //System.out.println(metadata);
                                }
                            }
                        });
                System.out.println("produced "+ i);
            });
            producer.close();
        }).start();
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
