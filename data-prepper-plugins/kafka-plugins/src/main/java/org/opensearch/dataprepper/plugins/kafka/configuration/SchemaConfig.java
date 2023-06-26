/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read user configuration values from
 * pipelines.yaml
 */

public class SchemaConfig {

    @JsonProperty("registry_url")
    private String registryURL;

    @JsonProperty("version")
    private int version;

    @JsonProperty("cluster_api_key")
    private String clusterApiKey;
    @JsonProperty("cluster_api_secret")
    private String clusterApiSecret;
    @JsonProperty("schema_registry_api_key")
    private String schemaRegistryApiKey;
    @JsonProperty("schema_registry_api_secret")
    private String schemaRegistryApiSecret;

    public String getRegistryURL() {
        return registryURL;
    }

    public void setRegistryURL(String registryURL) {
        this.registryURL = registryURL;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getClusterApiKey() {
        return clusterApiKey;
    }

    public String getClusterApiSecret() {
        return clusterApiSecret;
    }

    public String getSchemaRegistryApiKey() {
        return schemaRegistryApiKey;
    }

    public String getSchemaRegistryApiSecret() {
        return schemaRegistryApiSecret;
    }
}
