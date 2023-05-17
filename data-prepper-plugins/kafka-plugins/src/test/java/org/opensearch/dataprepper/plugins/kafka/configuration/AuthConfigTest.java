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

import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;


class AuthConfigTest {

    @Mock
    AuthConfig authConfig;

    @BeforeEach
    void setUp() throws FileNotFoundException {
        authConfig = new AuthConfig();
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
            authConfig = topicConfig.getAuthConfig();
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
    void testConfig() {
        assertThat(authConfig, notNullValue());
        assertThat(authConfig.getPlainTextAuthConfig(), notNullValue());
        assertThat(authConfig.getPlainTextAuthConfig(), hasProperty("username"));
        assertThat(authConfig.getPlainTextAuthConfig(), hasProperty("password"));
    }

    @Test
    void test_setters(){
        authConfig = new AuthConfig();
        PlainTextAuthConfig plainTextAuthConfig = mock(PlainTextAuthConfig.class);
        authConfig.setPlainTextAuthConfig(plainTextAuthConfig);

        assertEquals(plainTextAuthConfig, authConfig.getPlainTextAuthConfig());
    }
}