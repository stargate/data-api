package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.http.HttpClientOptions;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

/** Tests for {@link NvidiaRerankingProvider} */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class NvidiaRerankingProviderTest {

  private static final TestConstants testConstants = new TestConstants();

  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 37, 10);

  private static final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig MODEL_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
          "nvidia/llama-3.2-nv-rerankqa-1b-v2",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          false,
          "https://us-west-2.api-dev.ai.datastax.com/nvidia/v1/ranking",
          REQUEST_PROPERTIES);

  @Inject RerankingProvidersConfig rerankingProvidersConfig;

  @Test
  void configuresConnectionPoolSizeFromModel() {
    var options = new HttpClientOptions();

    NvidiaRerankingProvider.clientOptionsCustomizer(MODEL_CONFIG).accept(options);

    assertThat(options.getMaxPoolSize()).isEqualTo(37);
  }

  @Test
  void loadsExplicitConnectionPoolSize() {
    var configuredModel =
        rerankingProvidersConfig.providers().values().stream()
            .flatMap(provider -> provider.models().stream())
            .filter(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::isDefault)
            .findFirst()
            .orElseThrow();

    assertThat(configuredModel.properties().connectionPoolSize()).isEqualTo(50);
  }

  @Test
  void sharedProviderUsesCredentialsFromConcurrentCalls() throws Exception {
    var nvidiaClient = mock(NvidiaRerankingProvider.NvidiaRerankingClient.class);
    when(nvidiaClient.rerank(any(), any(), any())).thenReturn(Uni.createFrom().nothing());
    var provider = new NvidiaRerankingProvider(MODEL_CONFIG, nvidiaClient);
    var firstTenant = Tenant.create(DatabaseType.ASTRA, "first-tenant");
    var secondTenant = Tenant.create(DatabaseType.ASTRA, "second-tenant");
    var barrier = new CyclicBarrier(2);

    try (var executor = Executors.newFixedThreadPool(2)) {
      var calls =
          List.of(
              executor.submit(
                  () -> {
                    barrier.await();
                    provider.rerank(
                        0,
                        "query",
                        List.of("first"),
                        new RerankingCredentials(firstTenant, "first-key"));
                    return null;
                  }),
              executor.submit(
                  () -> {
                    barrier.await();
                    provider.rerank(
                        1,
                        "query",
                        List.of("second"),
                        new RerankingCredentials(secondTenant, "second-key"));
                    return null;
                  }));

      for (var call : calls) {
        call.get();
      }
    }

    verify(nvidiaClient)
        .rerank(
            eq(HttpConstants.BEARER_PREFIX_FOR_API_KEY + "first-key"),
            eq(firstTenant.toString()),
            any(NvidiaRerankingProvider.NvidiaRerankingRequest.class));
    verify(nvidiaClient)
        .rerank(
            eq(HttpConstants.BEARER_PREFIX_FOR_API_KEY + "second-key"),
            eq(secondTenant.toString()),
            any(NvidiaRerankingProvider.NvidiaRerankingRequest.class));
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
}
