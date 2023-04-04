# Kafka source

This is the Data Prepper Kafka source plugin that reads records from Kafka topic. It uses the consumer API provided by Kafka to read messages from the broker.

The Kafka source plugin supports OpenSearch 2.0.0 and greater.

## Usages

The Kafka source should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
pipeline:
  source:
    kafka:
      bootstrap_servers: 
       - localhost:9092
      topic:
       - my-topic 
      consumer_group:
        group_name: kafka-consumer-group
        group_id: DPKafkaProj
        workers: 3
        autocommit: false
        autocommit_interval: 5s
        session_timeout: 45s
        max_retry_attempts: 5
        auto_offset_reset: earliest
        thread_waiting_time: 1s
        max_record_fetch_time: 4s
        heart_beat_interval: 3s
        buffer_default_timeout: 5s
        fetch_max_bytes: 52428800
        fetch_max_wait: 500
        fetch_min_bytes: 1
        retry_backoff: 100s
        max_poll_interval: 300000s
        consumer_max_poll_records: 500
      schema:
        registry_url: http://localhost:8081/
        key_deserializer: org.apache.kafka.common.serialization.StringDeserializer 
        value_deserializer: org.apache.kafka.common.serialization.StringDeserializer
        schema_type: plaintext
        record_type: plaintext     
  sink:
    - stdout:
```

## Configuration

- `bootstrap_servers` (Required) : It is a Host/port to use for establishing the initial connection to the Kafka cluster.

- `topic` (Required) : The topic in which kafka source plugin associated with to read the messages. 

- `group_name`  (Required) : A consumer group name which this kafka consumer belongs to. 

- `group_id` (Required) : A consumer group id which this kafka consumer is associated with. 

- `workers` (Optional) : Number of multithreaded consumers. Defaults to `3`

- `autocommit` (Required) : If false the consumer's offset will not be periodically committed in the background. Defaults to `false`.

- `autocommit_interval` (Optional) : The frequency in seconds that the consumer offsets are auto-committed to Kafka. Defaults to `1s`.

- `session_timeout` (Optional) : The timeout used to detect client failures when using Kafka's group management. It is used for the rebalance.

- `max_retry_attempts` (Required) : maximum attempts to retry a failed request to a given topic partition. Defaults to `5`.

- `auto_offset_reset` (Optional) : automatically reset the offset to the earliest/latest offset. Defaults to `earliest`.

- `thread_waiting_time` (Optional) : It is the time for thread to wait until other thread completes the task and signal it.

- `max_record_fetch_time` (Optional) : maximum time to fetch the record from the topic.
Defaults to `4s`.

- `heart_beat_interval` (Optional) : The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities. Defaults to `1s`.

- `buffer_default_timeout` (Optional) :  Defaults to `1s`.

- `fetch_max_bytes` (Optional) : The maximum record batch size accepted by the broker. 
Defaults to `52428800`.

- `fetch_max_wait` (Optional) : The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement. Defaults to `500`.

- `fetch_min_bytes` (Optional) : The minimum amount of data the server should return for a fetch request. Defaults to `1`.

- `retry_backoff` (Optional) : The amount of time to wait before attempting to retry a failed request to a given topic partition.  Defaults to `5s`.

- `max_poll_interval` (Optional) : TThe maximum delay between invocations of poll() when using consumer group management. Defaults to `1s`.

- `consumer_max_poll_records` (Optional) : The maximum number of records returned in a single call to poll(). Defaults to `1s`.

### <a name="schema_configuration">schema Configuration</a>

- `key_deserializer` (Optional) : Deserialize a record value from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

- `value_deserializer` (Optional) : Deserialize a record key from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

- `schema_type` (Optional) : The type of schema format reading from the broker. This Kafka Consumer plugin supports String and json schema types. Defaults to `plaintext`.

- `record_type` (Optional) : The type of record format reading from the broker. This Kafka Consumer plugin supports String and json record types. Defaults to `plaintext`.


## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)