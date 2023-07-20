/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AuthProtocolConfig {
    @JsonProperty("plaintext")
    private String plaintext;
    @JsonProperty("ssl")
    private String ssl;

    public String getPlaintext() {
        return plaintext;
    }

    public String getSsl() {
        return ssl;
    }
}
