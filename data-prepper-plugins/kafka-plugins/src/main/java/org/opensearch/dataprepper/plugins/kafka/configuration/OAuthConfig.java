package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OAuthConfig {
    @JsonProperty("oauth_login_server")
    private String oauthLoginServer;
    @JsonProperty("oauth_login_endpoint")
    private String oauthLoginEndpoint;
    @JsonProperty("oauth_login_grant_type")
    private String oauthLoginGrantType;
    @JsonProperty("oauth_login_scope")
    private String oauthLoginScope;
    @JsonProperty("oauth_authorization_token")
    private String oauthAuthorizationToken;
    @JsonProperty("oauth_introspect_server")
    private String oauthIntrospectServer;
    @JsonProperty("oauth_introspect_endpoint")
    private String oauthIntrospectEndpoint;
    @JsonProperty("oauth_introspect_authorization_token")
    private String oauthIntrospectAuthorizationToken;

    public String getOauthLoginServer() {
        return oauthLoginServer;
    }

    public void setOauthLoginServer(String oauthLoginServer) {
        this.oauthLoginServer = oauthLoginServer;
    }

    public String getOauthLoginEndpoint() {
        return oauthLoginEndpoint;
    }

    public void setOauthLoginEndpoint(String oauthLoginEndpoint) {
        this.oauthLoginEndpoint = oauthLoginEndpoint;
    }

    public String getOauthLoginGrantType() {
        return oauthLoginGrantType;
    }

    public void setOauthLoginGrantType(String oauthLoginGrantType) {
        this.oauthLoginGrantType = oauthLoginGrantType;
    }

    public String getOauthLoginScope() {
        return oauthLoginScope;
    }

    public void setOauthLoginScope(String oauthLoginScope) {
        this.oauthLoginScope = oauthLoginScope;
    }

    public String getOauthIntrospectServer() {
        return oauthIntrospectServer;
    }

    public void setOauthIntrospectServer(String oauthIntrospectServer) {
        this.oauthIntrospectServer = oauthIntrospectServer;
    }

    public String getOauthIntrospectEndpoint() {
        return oauthIntrospectEndpoint;
    }

    public void setOauthIntrospectEndpoint(String oauthIntrospectEndpoint) {
        this.oauthIntrospectEndpoint = oauthIntrospectEndpoint;
    }

    public String getOauthAuthorizationToken() {
        return oauthAuthorizationToken;
    }

    public void setOauthAuthorizationToken(String oauthAuthorizationToken) {
        this.oauthAuthorizationToken = oauthAuthorizationToken;
    }

    public String getOauthIntrospectAuthorizationToken() {
        return oauthIntrospectAuthorizationToken;
    }

    public void setOauthIntrospectAuthorizationToken(String oauthIntrospectAuthorizationToken) {
        this.oauthIntrospectAuthorizationToken = oauthIntrospectAuthorizationToken;
    }
}
