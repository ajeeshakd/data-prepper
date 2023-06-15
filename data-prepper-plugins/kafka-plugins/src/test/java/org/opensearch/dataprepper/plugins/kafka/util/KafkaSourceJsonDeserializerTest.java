/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

public class KafkaSourceJsonDeserializerTest {

    private String topic;
    private byte[] data;

    private String error ="com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'Hello': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')";
    private KafkaSourceJsonDeserializer kafkaSourceJsonDeserializer;

    @BeforeEach
    void setUp() throws Exception {
        topic = "my-topic";
        data = new byte[] { 72, 101, 108, 108, 111, 32, 63, 63, 63,
                63, 63, 33 };
        kafkaSourceJsonDeserializer = new KafkaSourceJsonDeserializer();
    }

    @Test
    void testClose() {
        kafkaSourceJsonDeserializer.close();
    }

    @Test
    void testConfigure() {
        Map map = new HashMap();
        kafkaSourceJsonDeserializer.configure(map, true);
    }

    @Test
    void testDeserializeWithNull(){
        kafkaSourceJsonDeserializer.deserialize(topic,null);
    }

    @Test
    void testDeserialize(){
        byte[] dat = new byte[]{};
        kafkaSourceJsonDeserializer.deserialize(topic,dat) ;
    }
    @Test
    void testDeserializeWithException(){

        KafkaSourceJsonDeserializer des = new KafkaSourceJsonDeserializer();
        KafkaSourceJsonDeserializer spyHandler = spy(des);
            Throwable exception = assertThrows(org.apache.kafka.common.errors.SerializationException.class,
                    () -> spyHandler.deserialize(topic,data));
           // assertEquals(error, exception.getMessage());
        }

}
