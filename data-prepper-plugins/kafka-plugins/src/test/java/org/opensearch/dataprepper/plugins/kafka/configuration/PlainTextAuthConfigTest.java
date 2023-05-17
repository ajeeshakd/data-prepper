package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;

class PlainTextAuthConfigTest {

    @Mock
    PlainTextAuthConfig plainTextAuthConfig;

    @BeforeEach
    void setUp() throws FileNotFoundException {
        plainTextAuthConfig = new PlainTextAuthConfig();
        //Added to load Yaml file - Start
        Yaml yaml = new Yaml();
        FileReader reader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(reader);
        ObjectMapper mapper = new ObjectMapper();
        if(data instanceof Map){
            Map<Object, Object> propertyMap = (Map<Object, Object>) data;
            Map<Object, Object> nestedMap = new HashMap<>();
            iterateMap(propertyMap, nestedMap);
            List<TopicConfig> topicConfigList = (List<TopicConfig>) nestedMap.get("topics");
            mapper.registerModule(new JavaTimeModule());
            TopicsConfig topicsConfig = mapper.convertValue(topicConfigList.get(0), TopicsConfig.class);
            TopicConfig topicConfig = topicsConfig.getTopic();
            AuthConfig authConfig = topicConfig.getAuthConfig();
            plainTextAuthConfig = authConfig.getPlainTextAuthConfig();
        }
    }

    private static Map<Object, Object> iterateMap(Map<Object, Object> map, Map<Object, Object> nestedMap) {
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            String key = (String) entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                iterateMap((Map<Object, Object>) value, nestedMap); // call recursively for nested maps
            } else {
                nestedMap.put(key, value);
            }
        }
        return nestedMap;
    }

    @Test
    void testConfigValues_not_null() {
        assertThat(plainTextAuthConfig, notNullValue());
        assertThat(plainTextAuthConfig.getUsername(), notNullValue());
        assertThat(plainTextAuthConfig.getPassword(), notNullValue());

        assertEquals("admin", plainTextAuthConfig.getUsername());
        assertEquals("admin-secret", plainTextAuthConfig.getPassword());
    }

    @Test
    void testSetters(){
        plainTextAuthConfig = new PlainTextAuthConfig();
        plainTextAuthConfig.setUsername("test");
        plainTextAuthConfig.setPassword("test-secret");

        assertEquals("test", plainTextAuthConfig.getUsername());
        assertEquals("test-secret", plainTextAuthConfig.getPassword());
    }
}