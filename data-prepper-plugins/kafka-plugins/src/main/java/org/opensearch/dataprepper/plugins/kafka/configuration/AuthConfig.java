/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;


import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read authentication related configuration values from
 * pipelines.yaml
 */
public class AuthConfig {
    @JsonProperty("sasl_plaintext")
    PlainTextAuthConfig plainTextAuthConfig;

    public PlainTextAuthConfig getPlainTextAuthConfig() {
        return plainTextAuthConfig;
    }

    public void setPlainTextAuthConfig(PlainTextAuthConfig plainTextAuthConfig) {
        this.plainTextAuthConfig = plainTextAuthConfig;
    }
}
