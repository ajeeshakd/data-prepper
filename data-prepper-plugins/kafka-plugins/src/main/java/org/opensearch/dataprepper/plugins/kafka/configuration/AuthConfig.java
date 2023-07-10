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
    @JsonProperty("protocol")
    private AuthProtocolConfig authProtocolConfig;

    @JsonProperty("mechanism")
    private AuthMechanismConfig authMechanismConfig;

    public AuthProtocolConfig getAuthProtocolConfig() {
        return authProtocolConfig;
    }

    public AuthMechanismConfig getAuthMechanismConfig() {
        return authMechanismConfig;
    }
}
