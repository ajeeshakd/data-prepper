/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import java.util.List;

import org.opensearch.dataprepper.plugins.kafka.source.configuration.ConsumerGroupConfig;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.SchemaConfig;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSourceConfig {

  @JsonProperty("bootstrap_servers")
  @NotNull
  private List<String> bootStrapServers;

  @JsonProperty("topic")
  @NotNull
  private List<String> topic;

  @JsonProperty("consumer_group")
  @NotNull
  @Valid
  private ConsumerGroupConfig consumerGroupConfig;

  @JsonProperty("schema")
  @NotNull
  @Valid
  private SchemaConfig schemaConfig;

  public List<String> getBootStrapServers() {
    return bootStrapServers;
  }

  public void setBootStrapServers(List<String> bootStrapServers) {
    this.bootStrapServers = bootStrapServers;
  }

  public ConsumerGroupConfig getConsumerGroupConfig() {
    return consumerGroupConfig;
  }

  public void setConsumerGroupConfig(ConsumerGroupConfig consumerGroupConfig) {
    this.consumerGroupConfig = consumerGroupConfig;
  }

  public List<String> getTopic() {
    return topic;
  }

  public void setTopic(List<String> topic) {
    this.topic = topic;
  }

  public SchemaConfig getSchemaConfig() {
    return schemaConfig;
  }

  public void setSchemaConfig(SchemaConfig schemaConfig) {
    this.schemaConfig = schemaConfig;
  }

}
