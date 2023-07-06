/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;

import java.util.Base64;
import java.util.Properties;

/**
 * * This is static property configurer dedicated to authencation related information given in pipeline.yml
 */

public class AuthenticationPropertyConfigurer {

    private static final String SESSION_TIMEOUT_MS_CONFIG = "30000";

    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final String SASL_SECURITY_PROTOCOL = "security.protocol";

    private static final String SASL_JAS_CONFIG = "sasl.jaas.config";

    private static final String SASL_CALLBACK_HANDLER_CLASS = "sasl.login.callback.handler.class";

    private static final String SASL_JWKS_ENDPOINT_URL = "sasl.oauthbearer.jwks.endpoint.url";

    private static final String SASL_TOKEN_ENDPOINT_URL = "sasl.oauthbearer.token.endpoint.url";

    private static final String PLAINTEXT_JAASCONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username= \"%s\" password=  " +
            " \"%s\";";
    private static final String OAUTH_JAASCONFIG = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='"
            + "%s" + "' clientSecret='" + "%s" + "' scope='" + "%s" + "' OAUTH_LOGIN_SERVER='" + "%s" +
            "' OAUTH_LOGIN_ENDPOINT='" + "%s" + "' OAUT_LOGIN_GRANT_TYPE=" + "%s" +
            " OAUTH_LOGIN_SCOPE=%s OAUTH_AUTHORIZATION='Basic " + "%s" + "';";

    private static final String INSTROSPECT_SERVER_PROPERTIES = " OAUTH_INTROSPECT_SERVER='"
            + "%s" + "' OAUTH_INTROSPECT_ENDPOINT='" + "%s" + "' " +
            "OAUTH_INTROSPECT_AUTHORIZATION='Basic " + "%s";

    private static final String PLAIN_MECHANISM = "PLAIN";

    private static final String SASL_PLAINTEXT_PROTOCOL = "SASL_PLAINTEXT";


    public static void setSaslPlainTextProperties(final KafkaSourceConfig KafkaSourceConfig,
                                                  final Properties properties) {

        String username = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig().getUsername();
        String password = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig().getPassword();
        properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        properties.put(SASL_JAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
        properties.put(SASL_SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
    }

    public static void setOauthProperties(final KafkaSourceConfig KafkaSourceConfig,
                                          final Properties properties) {
        String securityProtocol= "";
        String oauthClientId = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthClientId();
        String oauthClientSecret = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthClientSecret();
        String oauthLoginServer = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginServer();
        String oauthLoginEndpoint = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginEndpoint();
        String oauthLoginGrantType = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginGrantType();
        String oauthLoginScope = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginScope();
        String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        String oauthIntrospectEndpoint = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthIntrospectEndpoint();
        String tokenEndPointURL = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthTokenEndpointURL();
        String saslMechanism = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthSaslMechanism();
        if(StringUtils.isNotEmpty(KafkaSourceConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext())) {
            securityProtocol= KafkaSourceConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext();
        }else if(StringUtils.isNotEmpty(KafkaSourceConfig.getAuthConfig().getAuthProtocolConfig().getSsl())){
            securityProtocol= KafkaSourceConfig.getAuthConfig().getAuthProtocolConfig().getSsl();
        }
        String loginCallBackHandler = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthSaslLoginCallbackHandlerClass();
        String oauthJwksEndpointURL = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthJwksEndpointURL();
        String introspectServer = KafkaSourceConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthIntrospectServer();


        properties.put(SASL_MECHANISM, saslMechanism);
        properties.put(SASL_SECURITY_PROTOCOL, securityProtocol);
        properties.put(SASL_TOKEN_ENDPOINT_URL, tokenEndPointURL);
        properties.put(SASL_CALLBACK_HANDLER_CLASS, loginCallBackHandler);
        if (oauthJwksEndpointURL != null && !oauthJwksEndpointURL.isEmpty() && !oauthJwksEndpointURL.isBlank()) {
            properties.put(SASL_JWKS_ENDPOINT_URL, oauthJwksEndpointURL);
        }

        String instrospect_properties = "";
        if (oauthJwksEndpointURL != null && !oauthIntrospectEndpoint.isBlank() && !oauthIntrospectEndpoint.isEmpty()) {
            instrospect_properties = String.format(INSTROSPECT_SERVER_PROPERTIES, introspectServer, oauthIntrospectEndpoint, oauthAuthorizationToken);
        }

        String jass_config = String.format(OAUTH_JAASCONFIG, oauthClientId, oauthClientSecret, oauthLoginScope, oauthLoginServer,
                oauthLoginEndpoint, oauthLoginGrantType, oauthLoginScope, oauthAuthorizationToken, instrospect_properties);

        properties.put(SASL_JAS_CONFIG, jass_config);
    }
}

