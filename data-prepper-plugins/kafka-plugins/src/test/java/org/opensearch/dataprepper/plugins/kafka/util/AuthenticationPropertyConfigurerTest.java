/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

@ExtendWith(MockitoExtension.class)
public class AuthenticationPropertyConfigurerTest {



    KafkaSourceConfig kafkaSourceConfig;


    @BeforeEach
    public void setUp() throws IOException {

        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        if(data instanceof Map){
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sinkeMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sinkeMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
        }
    }

    @Test
     public void testSetSaslPlainTextProperties(){
        Properties props=new Properties();
        AuthenticationPropertyConfigurer.setSaslPlainTextProperties(kafkaSourceConfig,props);
        Assertions.assertEquals("PLAIN",props.getProperty("sasl.mechanism"));
     }
    @Test
    public void testSetSaslOauthProperties(){
        Properties props=new Properties();
        AuthenticationPropertyConfigurer.setOauthProperties(kafkaSourceConfig,props);
        Assertions.assertEquals("OAUTHBEARER",props.getProperty("sasl.mechanism"));
    }
}
