/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import java.util.List;


import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class KafkaSourceConfig {

  @JsonProperty("bootstrap_servers")
  @NotNull
  @Size(min = 1, message = "Bootstrap servers can't be empty")
  private List<String> bootStrapServer;

  @JsonProperty("cluster_api_key")
  private String clusterApiKey;
  @JsonProperty("cluster_api_secret")
  private String clusterApiSecret;
  @JsonProperty("serde_format")
  private String serdeFormat;

  @JsonProperty("client_dns_lookup")
  private String clientDnsLookup;

    @JsonProperty("topics")
    @NotNull
    @Size(min = 1, max = 10, message = "The number of Topics should be between 1 and 10")
    private List<TopicConfig> topics;

  @JsonProperty("schema")
  @Valid
  private SchemaConfig schemaConfig;

  @JsonProperty("authentication")
  private AuthConfig authConfig;

  public String getClusterApiKey() {
    return clusterApiKey;
  }

  public String getClusterApiSecret() {
    return clusterApiSecret;
  }

  public List<String> getBootStrapServers() {
    return bootStrapServer;
  }

  public String getSerdeFormat() {
    return serdeFormat;
  }

  public String getClientDnsLookup() {
    return clientDnsLookup;
  }

  public List<TopicConfig> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicConfig> topics) {
        this.topics = topics;
    }

  public SchemaConfig getSchemaConfig() {
    return schemaConfig;
  }

  public void setSchemaConfig(SchemaConfig schemaConfig) {
    this.schemaConfig = schemaConfig;
  }

  public AuthConfig getAuthConfig() {
    return authConfig;
  }

  public void setAuthConfig(AuthConfig authConfig) {
    this.authConfig = authConfig;
  }
}
