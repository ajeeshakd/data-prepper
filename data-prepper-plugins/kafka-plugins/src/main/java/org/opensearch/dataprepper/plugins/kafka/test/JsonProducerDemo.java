/*
 * Copyright (c) 2018. Prashant Kumar Pandey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.opensearch.dataprepper.plugins.kafka.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka producer demo to send NSE data events from file
 * Reads CSV data from file and converts into Json objects
 * Sends Json messages to Kafka producer
 * Starts one thread for each file
 *
 * @author prashant
 * @author www.learningjournal.guru
 */
public class JsonProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(JsonProducerDemo.class);
    private static final String kafkaConfig = "/kafka.properties";

    /**
     * private static method to read data from given dataFile
     *
     * @param dataFile data file name in resource folder
     * @return List of StockData Instance
     * @throws IOException, NullPointerException
     */
    private static List<StockData> getStocks(String dataFile) throws IOException {

        File file = new File(dataFile);
        MappingIterator<StockData> stockDataIterator = new CsvMapper().readerWithTypedSchemaFor(StockData.class).readValues(file);
        return stockDataIterator.readAll();
    }

    /**
     * Application entry point
     * you must provide the topic name and at least one event file
     *
     * @param args topicName (Name of the Kafka topic) list of files (list of files in the classpath)
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        final ObjectMapper objectMapper = new ObjectMapper();
        List<Thread> dispatchers = new ArrayList<>();

        /*if (args.length < 2) {
            System.out.println("Please provide command line arguments: topicName EventFiles");
            System.exit(-1);
        }*/
        /*args[0]="my-topic";
        args[1]= "/data/NSE05NOV2018BHAV.csv";*/
        //args[2]="/data/NSE06NOV2018BHAV.csv";

        String topicName = "my-topic-1";
        //String[] eventFiles = Arrays.copyOfRange(args, 1, args.length);
        String[] eventFiles = {"C:\\Users\\AJ20444591\\Desktop\\DataPrepper\\Projects\\json-producer\\src\\main\\resources\\data\\NSE05NOV2018BHAV.csv",
                               "C:\\Users\\AJ20444591\\Desktop\\DataPrepper\\Projects\\json-producer\\src\\main\\resources\\data\\NSE06NOV2018BHAV.csv"};
        String[] eventFiles1 = {"C:\\Users\\AJ20444591\\Desktop\\DataPrepper\\Projects\\json-producer\\src\\main\\resources\\data\\SALES_DATA.csv"};
        List<JsonNode>[] stockArrayOfList = new List[eventFiles.length];
        for (int i = 0; i < stockArrayOfList.length; i++) {
            stockArrayOfList[i] = new ArrayList<>();
        }

        logger.trace("Creating Kafka producer...");
        Properties properties = new Properties();
        try {
            //InputStream kafkaConfigStream = ClassLoader.class.getResourceAsStream(kafkaConfig);
            //properties.load(kafkaConfigStream);
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-source-consumer-1");
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "DPKafkaProj-1");
            //Set autocommit to false so you can execute it again for the same set of messages
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
            properties.put("sasl.mechanism", "PLAIN");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + "admin" + "\" password=\"" + "admin-secret" + "\";");
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        } catch (Exception e) {
            logger.error("Cannot open Kafka config " + kafkaConfig);
            throw new RuntimeException(e);
        }

        KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < eventFiles.length; i++) {
                for (StockData s : getStocks(eventFiles[i])) {
                    stockArrayOfList[i].add(objectMapper.valueToTree(s));
                }
                dispatchers.add(new Thread(new Dispatcher(producer, topicName, eventFiles[i], stockArrayOfList[i]), eventFiles[i]));
                dispatchers.get(i).start();
            }
        } catch (Exception e) {
            logger.error("Exception in reading data file.");
            producer.close();
            throw new RuntimeException(e);
        }

        //Wait for threads
        try {
            for (Thread t : dispatchers) {
                t.join();
            }
        } catch (InterruptedException e) {
            logger.error("Thread Interrupted ");
            throw new RuntimeException(e);
        } finally {
            producer.close();
            logger.info("Finished Application - Closing Kafka Producer.");
        }
    }
}
