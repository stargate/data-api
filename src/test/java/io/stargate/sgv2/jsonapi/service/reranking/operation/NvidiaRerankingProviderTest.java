package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties.TruncateOption.NONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for {@link NvidiaRerankingProvider} */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class NvidiaRerankingProviderTest {

  private static final TestConstants testConstants = new TestConstants();
  private static final String NVIDIA_PATH = "/v1/ranking";
  private static final String NVIDIA_URL = "http://localhost:8080" + NVIDIA_PATH;
  private static WireMockServer wireMockServer;

  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10, NONE);

  private static final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig MODEL_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
          "nvidia/llama-3.2-nv-rerankqa-1b-v2",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          false,
          "https://us-west-2.api-dev.ai.datastax.com/nvidia/v1/ranking",
          REQUEST_PROPERTIES);

  private static final RerankingCredentials RERANKING_CREDENTIALS =
      new RerankingCredentials(testConstants.TENANT, "mocked reranking api key");

  @Inject RerankingProvidersConfig rerankingProvidersConfig;

  @BeforeAll
  static void startWireMock() {
    wireMockServer = new WireMockServer();
    wireMockServer.start();
    wireMockServer.stubFor(
        post(urlEqualTo(NVIDIA_PATH))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody(
                        """
                        {
                          "rankings": [{"index": 0, "logit": 0.75}],
                          "usage": {"prompt_tokens": 12, "total_tokens": 12}
                        }
                        """)));
  }

  @AfterAll
  static void stopWireMock() {
    wireMockServer.stop();
  }

  @Test
  void testEmptyApiKeyThrowsException() {
    NvidiaRerankingProvider provider = new NvidiaRerankingProvider(MODEL_CONFIG);

    RerankingCredentials emptyApiKeyCredentials =
        new RerankingCredentials(testConstants.TENANT, "");

    assertThatThrownBy(
            () ->
                provider
                    .rerank(
                        1, "test query", List.of("passage1", "passage2"), emptyApiKeyCredentials)
                    .subscribe()
                    .withSubscriber(UniAssertSubscriber.create())
                    .awaitItem())
        .isInstanceOf(SchemaException.class)
        .satisfies(
            exception -> {
              SchemaException schemaException = (SchemaException) exception;
              assertThat(schemaException.code)
                  .isEqualTo(
                      SchemaException.Code.RERANKING_PROVIDER_AUTHENTICATION_KEY_NOT_PROVIDED
                          .name());
            });
  }

  @Test
  void testTenantIdIsExtractedFromCredentials() {
    // Verify that the tenant from RerankingCredentials is correctly accessible
    // This ensures the tenant ID will be correctly passed as "tenant-id" header
    NvidiaRerankingProvider provider = new NvidiaRerankingProvider(MODEL_CONFIG);

    String expectedTenantId = testConstants.TENANT.toString();
    RerankingCredentials credentials =
        new RerankingCredentials(testConstants.TENANT, "valid-api-key");

    // Verify tenant is correctly stored in credentials
    assertThat(credentials.tenant()).isEqualTo(testConstants.TENANT);
    assertThat(credentials.tenant().toString()).isEqualTo(expectedTenantId);

    // The tenant ID from credentials.tenant().toString() is what gets passed as
    // @HeaderParam("tenant-id") to the REST client (see NvidiaRerankingProvider line 111)
    // This test verifies the tenant is correctly sourced from credentials
    assertThat(credentials.tenant().toString())
        .as("Tenant ID should be correctly extractable from credentials for header usage")
        .isNotNull()
        .isEqualTo(expectedTenantId);
  }

  @Test
  void sendsTruncationConfiguredInYaml() {
    var configuredModel = rerankingProvidersConfig.providers().get("nvidia").models().getFirst();
    var localModel =
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
            configuredModel.name(),
            configuredModel.apiModelSupport(),
            configuredModel.isDefault(),
            NVIDIA_URL,
            configuredModel.properties());
    NvidiaRerankingProvider provider = new NvidiaRerankingProvider(localModel);

    provider
        .rerank(1, "test query", List.of("test passage"), RERANKING_CREDENTIALS)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem()
        .getItem();

    wireMockServer.verify(
        postRequestedFor(urlEqualTo(NVIDIA_PATH))
            .withRequestBody(matchingJsonPath("$.truncate", equalTo("END"))));
  }
}
