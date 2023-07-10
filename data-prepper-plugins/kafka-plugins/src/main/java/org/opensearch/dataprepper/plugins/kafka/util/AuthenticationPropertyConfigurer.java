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
 * * This is static property configure dedicated to authentication related information given in pipeline.yml
 */

public class AuthenticationPropertyConfigurer {

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
    private static final String OAUTHBEARER_MECHANISM= "OAUTHBEARER";

    private static final String SASL_PLAINTEXT_PROTOCOL = "SASL_PLAINTEXT";

    private static final String REGISTRY_BASIC_AUTH_USER_INFO = "schema.registry.basic.auth.user.info";


    public static void setSaslPlainTextProperties(final KafkaSourceConfig kafkaSourConfig,
                                                  final Properties properties) {
        final String saslMechanism;
        String username = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig().getUsername();
        String password = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig().getPassword();
        if(kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig()!=null){
            //properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
            properties.put(SASL_MECHANISM,
                    kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getPlainTextAuthConfig().getSaslMechanism());
        }
       // final String securityProtocol = kafkaSourConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext();
        if(kafkaSourConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext() != null){
            //properties.put(SASL_SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
            properties.put(SASL_SECURITY_PROTOCOL,kafkaSourConfig.getAuthConfig().getAuthProtocolConfig().getPlaintext());
        }else if(kafkaSourConfig.getAuthConfig().getAuthProtocolConfig().getSsl() != null){
            properties.put(SASL_SECURITY_PROTOCOL,kafkaSourConfig.getAuthConfig().getAuthProtocolConfig().getSsl());
        }

       // properties.put(SASL_MECHANISM, PLAIN_MECHANISM);
        properties.put(SASL_JAS_CONFIG, String.format(PLAINTEXT_JAASCONFIG, username, password));
        //properties.put(SASL_SECURITY_PROTOCOL, SASL_PLAINTEXT_PROTOCOL);
       // properties.put(SASL_SECURITY_PROTOCOL, securityProtocol);
    }

    public static void setOauthProperties(final KafkaSourceConfig kafkaSourConfig,
                                          final Properties properties) {
        final String oauthClientId = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthClientId();
        final String oauthClientSecret = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthClientSecret();
        final String oauthLoginServer = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginServer();
        final String oauthLoginEndpoint = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginEndpoint();
        final String oauthLoginGrantType = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginGrantType();
        final String oauthLoginScope = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthLoginScope();
        final String oauthAuthorizationToken = Base64.getEncoder().encodeToString((oauthClientId + ":" + oauthClientSecret).getBytes());
        final String oauthIntrospectEndpoint = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthIntrospectEndpoint();
        final String tokenEndPointURL = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthTokenEndpointURL();
        final String saslMechanism = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthSaslMechanism();
        final String securityProtocol = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthSecurityProtocol();
        final String loginCallBackHandler = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthSaslLoginCallbackHandlerClass();
        final String oauthJwksEndpointURL = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthJwksEndpointURL();
        final String introspectServer = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getOauthIntrospectServer();


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

        if ("USER_INFO".equalsIgnoreCase(kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getCredentialsSource())) {
            final String apiKey = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getApiKey();
            final String apiSecret = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getApiSecret();
            final String extensionLogicalCluster = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getExtensionLogicalCluster();
            final String extensionIdentityPoolId = kafkaSourConfig.getAuthConfig().getAuthMechanismConfig().getoAuthConfig().getExtensionIdentityPoolId();
            properties.put(REGISTRY_BASIC_AUTH_USER_INFO, apiKey + ":" + apiSecret);
            String extensionValue = "extension_logicalCluster= \"%s\" extension_identityPoolId=  " + " \"%s\";";
            jass_config= jass_config.replace(";"," ");
            jass_config+=String.format(extensionValue, extensionLogicalCluster, extensionIdentityPoolId);
        }

        properties.put(SASL_JAS_CONFIG, jass_config);
    }
}

