/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthMechanismConfig {
    @JsonProperty("plain")
    private PlainTextAuthConfig plainTextAuthConfig;
    @JsonProperty("oauth")
    private OAuthConfig oAuthConfig;

    public PlainTextAuthConfig getPlainTextAuthConfig() {
        return plainTextAuthConfig;
    }

    public OAuthConfig getoAuthConfig() {
        return oAuthConfig;
    }
}
