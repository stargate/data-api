package io.stargate.sgv2.jsonapi.api.request;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class EmbeddingCredentialsSupplierTest {

  @Inject HttpConstants httpConstants;

  RequestContext requestContext;
  RequestContext.HttpHeaderAccess httpHeaderAccess;
  EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig;
  EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig noneAuthConfig;
  EmbeddingCredentialsSupplier supplier;

  @BeforeEach
  void setUp() {
    requestContext = mock(RequestContext.class);
    httpHeaderAccess = mock(RequestContext.HttpHeaderAccess.class);
    providerConfig = mock(EmbeddingProvidersConfig.EmbeddingProviderConfig.class);
    noneAuthConfig =
        mock(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig.class);

    supplier =
        new EmbeddingCredentialsSupplier(
            httpConstants.authToken(),
            httpConstants.embeddingApiKey(),
            httpConstants.embeddingAccessId(),
            httpConstants.embeddingSecretId());

    when(requestContext.getHttpHeaders()).thenReturn(httpHeaderAccess);
  }

  @Test
  public void shouldPassThroughAuthTokenWhenAllConditionsMet() {
    // not providing "x-embedding-api-key" and use auth token
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey())).thenReturn(null);
    when(httpHeaderAccess.getHeader(httpConstants.authToken())).thenReturn("astra-auth-token");
    // Provider config is available, Provider supports NONE auth and it's enabled
    when(providerConfig.supportedAuthentications())
        .thenReturn(
            Collections.singletonMap(
                EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE,
                noneAuthConfig));
    when(noneAuthConfig.enabled()).thenReturn(true);
    // Provider has authTokenPassThroughForNoneAuth set to true
    when(providerConfig.authTokenPassThroughForNoneAuth()).thenReturn(true);
    // no collection auth config
    supplier.withAuthConfigFromCollection(null);

    // Act
    EmbeddingCredentials credentials = supplier.create(requestContext, providerConfig);

    // Assert
    assertThat(credentials.apiKey()).contains("astra-auth-token");
    assertThat(credentials.accessId()).isEmpty();
    assertThat(credentials.secretId()).isEmpty();
  }

  @Test
  void shouldUseExplicitEmbeddingApiKeyWhenProvided() {
    // Arrange
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey()))
        .thenReturn("embedding-api-key");
    when(httpHeaderAccess.getHeader(httpConstants.embeddingAccessId())).thenReturn("access-id");
    when(httpHeaderAccess.getHeader(httpConstants.embeddingSecretId())).thenReturn("secret-id");

    // Act
    EmbeddingCredentials credentials = supplier.create(requestContext, providerConfig);

    // Assert
    assertThat(credentials.apiKey()).contains("embedding-api-key");
    assertThat(credentials.accessId()).contains("access-id");
    assertThat(credentials.secretId()).contains("secret-id");
  }

  @Test
  void shouldUseExplicitEmbeddingApiKeyIfPassThroughIsFalse() {
    // Arrange
    // User explicitly sends the header, but its value is null (or header not present)
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey())).thenReturn(null);
    when(httpHeaderAccess.getHeader(httpConstants.embeddingAccessId())).thenReturn("access-id");
    when(httpHeaderAccess.getHeader(httpConstants.embeddingSecretId())).thenReturn("secret-id");

    // Set passThrough to false, make one condition fail
    when(providerConfig.authTokenPassThroughForNoneAuth()).thenReturn(false);

    // Act
    EmbeddingCredentials credentials = supplier.create(requestContext, providerConfig);

    // Assert
    assertThat(credentials.apiKey()).isEmpty();
    assertThat(credentials.accessId()).contains("access-id");
    assertThat(credentials.secretId()).contains("secret-id");
  }

  @Test
  void shouldNotPassThroughIfNoneAuthNotSupportedByProvider() {
    // Provide auth token
    when(httpHeaderAccess.getHeader(httpConstants.authToken())).thenReturn("astra-auth-token");
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey())).thenReturn(null);
    // No NONE auth
    when(providerConfig.supportedAuthentications()).thenReturn(Collections.emptyMap());
    when(providerConfig.authTokenPassThroughForNoneAuth()).thenReturn(true);

    // Act
    EmbeddingCredentials credentials = supplier.create(requestContext, providerConfig);

    // Assert - not replaced with auth token
    assertThat(credentials.apiKey()).isEmpty();
  }

  @Test
  void shouldNotPassThroughIfNoneAuthSupportedButNotEnabled() {
    // Provide auth token
    when(httpHeaderAccess.getHeader(httpConstants.authToken())).thenReturn("astra-auth-token");
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey())).thenReturn(null);
    // NONE auth present but disabled
    when(providerConfig.supportedAuthentications())
        .thenReturn(
            Collections.singletonMap(
                EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE,
                noneAuthConfig));
    when(noneAuthConfig.enabled()).thenReturn(false);
    when(providerConfig.authTokenPassThroughForNoneAuth()).thenReturn(true);

    // Act
    EmbeddingCredentials credentials = supplier.create(requestContext, providerConfig);

    // Assert - not replaced with auth token
    assertThat(credentials.apiKey()).isEmpty();
  }

  @Test
  void shouldNotPassThroughIfCollectionHasItsOwnAuthConfig() {
    // Provide auth token
    when(httpHeaderAccess.getHeader(httpConstants.authToken())).thenReturn("astra-auth-token");
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey())).thenReturn(null);
    // Provider config is available, Provider supports NONE auth and it's enabled
    when(providerConfig.supportedAuthentications())
        .thenReturn(
            Collections.singletonMap(
                EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE,
                noneAuthConfig));
    when(noneAuthConfig.enabled()).thenReturn(true);
    // Provider has authTokenPassThroughForNoneAuth set to true
    when(providerConfig.authTokenPassThroughForNoneAuth()).thenReturn(true);
    // Collection has some auth config
    supplier.withAuthConfigFromCollection(Map.of("providerKey", "shared_creds.providerKey"));

    // Act
    EmbeddingCredentials credentials = supplier.create(requestContext, providerConfig);

    // Assert - not replaced with auth token
    assertThat(credentials.apiKey()).isEmpty();
  }

  @Test
  void shouldNotPassThroughIfProviderConfigIsNull() {
    // Arrange
    when(httpHeaderAccess.getHeader(httpConstants.embeddingApiKey())).thenReturn(null);
    when(httpHeaderAccess.getHeader(httpConstants.authToken())).thenReturn("stargate-auth-token");

    // Act: Pass null for providerConfig
    EmbeddingCredentials credentials = supplier.create(requestContext, null);

    // Assert - not replaced with auth token
    assertThat(credentials.apiKey()).isEmpty(); // Falls back to standard credential resolution
  }
}
