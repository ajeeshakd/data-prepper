/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;


import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * * A helper class that helps to read auth related configuration values from
 * pipelines.yaml
 */
public class AuthConfig {
    @JsonProperty("sasl_plaintext")
    private PlainTextAuthConfig plainTextAuthConfig;

    @JsonProperty("sasl_oauth")
    private OAuthConfig authConfig;

    public OAuthConfig getAuthConfig() {
        return authConfig;
    }

    public void setAuthConfig(OAuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    public PlainTextAuthConfig getPlainTextAuthConfig() {
        return plainTextAuthConfig;
    }
}
