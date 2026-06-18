package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.syncservice.SyncServiceClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SyncServiceCredentialResolvingProviderTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  private SyncServiceClient syncServiceClient;
  private TestEmbeddingProvider delegate;
  private String authToken;
  private EmbeddingCredentials emptyCreds;

  @BeforeEach
  void setUp() {
    syncServiceClient = mock(SyncServiceClient.class);
    delegate = spy(new TestEmbeddingProvider());
    authToken = TEST_CONSTANTS.AUTH_TOKEN;
    emptyCreds =
        new EmbeddingCredentials(
            TEST_CONSTANTS.TENANT,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
  }

  private SyncServiceCredentialResolvingProvider createProvider(
      Map<String, String> authentication) {
    return new SyncServiceCredentialResolvingProvider(
        delegate, syncServiceClient, authentication, TEST_CONSTANTS.TENANT, authToken);
  }

  /** Captures the EmbeddingCredentials passed to the spy delegate's vectorize call. */
  private EmbeddingCredentials captureResolvedCredentials() {
    var captor = ArgumentCaptor.forClass(EmbeddingCredentials.class);
    verify(delegate).vectorize(anyInt(), any(), captor.capture(), any());
    return captor.getValue();
  }

  @Nested
  class Vectorize {

    @Test
    void shouldResolveProviderKeyAndPassToDelegate() {
      Map<String, String> authentication = Map.of("providerKey", "my-openai-cred");

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("my-openai-cred")))
          .thenReturn(Uni.createFrom().item(Map.of("my-openai-cred", "resolved-api-key-secret")));

      var provider = createProvider(authentication);

      var response =
          provider
              .vectorize(
                  1, List.of("test text"), emptyCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(response).isNotNull();
      assertThat(response.batchId()).isEqualTo(1);
      assertThat(response.embeddings()).hasSize(1);

      // Verify the resolved credentials passed to delegate
      var resolved = captureResolvedCredentials();
      assertThat(resolved.apiKey()).contains("resolved-api-key-secret");
      assertThat(resolved.accessId()).isEmpty();
      assertThat(resolved.secretId()).isEmpty();
      assertThat(resolved.authToken()).contains(authToken);
    }

    @Test
    void shouldResolveMultipleCredentialsAndMapCorrectly() {
      // AWS Bedrock style: accessId + secretKey
      Map<String, String> authentication =
          Map.of(
              "accessId", "my-access-cred",
              "secretKey", "my-secret-cred");

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("my-access-cred")))
          .thenReturn(Uni.createFrom().item(Map.of("my-access-cred", "resolved-access-id")));

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("my-secret-cred")))
          .thenReturn(Uni.createFrom().item(Map.of("my-secret-cred", "resolved-secret-key")));

      var provider = createProvider(authentication);

      var response =
          provider
              .vectorize(
                  1, List.of("test text"), emptyCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(response).isNotNull();

      // Verify credentials are mapped to the correct fields
      var resolved = captureResolvedCredentials();
      assertThat(resolved.apiKey()).isEmpty();
      assertThat(resolved.accessId()).contains("resolved-access-id");
      // secretKey maps to secretId field in EmbeddingCredentials
      assertThat(resolved.secretId()).contains("resolved-secret-key");
    }

    @Test
    void shouldPreferSecretKeyOverSecretId() {
      // When both secretKey and secretId are in authentication, secretKey takes precedence
      // because buildEmbeddingCredentials checks containsKey("secretKey") first
      Map<String, String> authentication = Map.of("secretKey", "my-secret-key-cred");

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("my-secret-key-cred")))
          .thenReturn(
              Uni.createFrom().item(Map.of("my-secret-key-cred", "resolved-via-secret-key")));

      var provider = createProvider(authentication);

      provider
          .vectorize(1, List.of("text"), emptyCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      var resolved = captureResolvedCredentials();
      assertThat(resolved.secretId()).contains("resolved-via-secret-key");
    }

    @Test
    void shouldFallBackToSecretIdWhenNoSecretKey() {
      Map<String, String> authentication = Map.of("secretId", "my-secret-id-cred");

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("my-secret-id-cred")))
          .thenReturn(Uni.createFrom().item(Map.of("my-secret-id-cred", "resolved-via-secret-id")));

      var provider = createProvider(authentication);

      provider
          .vectorize(1, List.of("text"), emptyCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      var resolved = captureResolvedCredentials();
      assertThat(resolved.secretId()).contains("resolved-via-secret-id");
    }

    @Test
    void shouldSkipResolutionWhenApiKeyHeaderPresent() {
      var provider = createProvider(Map.of("providerKey", "my-cred"));

      // TEST_CONSTANTS.EMBEDDING_CREDENTIALS has apiKey set — should bypass SyncService
      var response =
          provider
              .vectorize(
                  1,
                  List.of("test text"),
                  TEST_CONSTANTS.EMBEDDING_CREDENTIALS,
                  EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(response).isNotNull();
      verifyNoInteractions(syncServiceClient);
    }

    @Test
    void shouldSkipResolutionWhenAccessIdHeaderPresent() {
      var provider = createProvider(Map.of("providerKey", "my-cred"));

      var headerCreds =
          new EmbeddingCredentials(
              TEST_CONSTANTS.TENANT,
              Optional.empty(),
              Optional.of("header-access-id"),
              Optional.empty(),
              Optional.empty());

      provider
          .vectorize(
              1, List.of("test text"), headerCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      verifyNoInteractions(syncServiceClient);
    }

    @Test
    void shouldSkipResolutionWhenSecretIdHeaderPresent() {
      var provider = createProvider(Map.of("providerKey", "my-cred"));

      var headerCreds =
          new EmbeddingCredentials(
              TEST_CONSTANTS.TENANT,
              Optional.empty(),
              Optional.empty(),
              Optional.of("header-secret-id"),
              Optional.empty());

      provider
          .vectorize(
              1, List.of("test text"), headerCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      verifyNoInteractions(syncServiceClient);
    }

    @Test
    void shouldPropagateErrorFromSyncService() {
      Map<String, String> authentication = Map.of("providerKey", "bad-cred");

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("bad-cred")))
          .thenReturn(
              Uni.createFrom()
                  .failure(
                      EmbeddingProviderException.Code
                          .EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE
                          .get("errorMessage", "Credential not found")));

      var provider = createProvider(authentication);

      var failure =
          provider
              .vectorize(
                  1, List.of("test text"), emptyCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure).isInstanceOf(EmbeddingProviderException.class);
    }

    @Test
    void shouldProduceEmptyCredentialsWhenKeyMissingFromResponse() {
      // SyncService returns a map that does NOT contain the expected credential name
      Map<String, String> authentication = Map.of("providerKey", "my-cred");

      when(syncServiceClient.getCredential(
              eq(authToken), eq(TEST_CONSTANTS.TENANT), eq("custom"), eq("my-cred")))
          .thenReturn(Uni.createFrom().item(Map.of("wrong-key", "some-value")));

      var provider = createProvider(authentication);

      provider
          .vectorize(1, List.of("text"), emptyCreds, EmbeddingProvider.EmbeddingRequestType.INDEX)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      // Credential was not resolved — apiKey should be empty
      var resolved = captureResolvedCredentials();
      assertThat(resolved.apiKey()).isEmpty();
    }
  }

  @Nested
  class MaxBatchSize {

    @Test
    void shouldDelegateMaxBatchSize() {
      var provider = createProvider(Map.of("providerKey", "cred"));
      assertThat(provider.maxBatchSize()).isEqualTo(delegate.maxBatchSize());
    }
  }

  @Nested
  class NameForMetrics {

    @Test
    void shouldDelegateNameForMetrics() {
      var provider = createProvider(Map.of("providerKey", "cred"));
      // delegate is a TestEmbeddingProvider using ModelProvider.CUSTOM → apiName "custom"
      assertThat(provider.nameForMetrics()).isEqualTo("custom");
      assertThat(provider.nameForMetrics()).isEqualTo(delegate.nameForMetrics());
    }
  }
}
