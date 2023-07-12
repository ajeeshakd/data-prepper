/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read sasl plaintext auth configuration values from
 * pipelines.yaml
 */
public class PlainTextAuthConfig {

    @JsonProperty("cluster_api_key")
    private String clusterApiKey;

    @JsonProperty("cluster_api_secret")
    private String clusterApiSecret;

    @JsonProperty("sasl_mechanism")
    private String saslMechanism;

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getClusterApiKey() {
        return clusterApiKey;
    }

    public String getClusterApiSecret() {
        return clusterApiSecret;
    }
}
