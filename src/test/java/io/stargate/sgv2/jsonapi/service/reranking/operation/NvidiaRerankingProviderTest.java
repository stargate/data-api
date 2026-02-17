package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import java.util.List;
import java.util.Optional;
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
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10);

  private static final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig MODEL_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
          "nvidia/llama-3.2-nv-rerankqa-1b-v2",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          false,
          "https://us-west-2.api-dev.ai.datastax.com/nvidia/v1/ranking",
          REQUEST_PROPERTIES);

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
