package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RerankingProviderFactory}, in particular that the {@code custom} provider (an
 * externally hosted, NIM-compatible reranking endpoint registered via the reranking providers
 * config) is wired to the NIM wire-format client with correct provider attribution.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class RerankingProviderFactoryTest {

  private static final TestConstants testConstants = new TestConstants();

  private static final String CUSTOM_MODEL_NAME = "my-org/my-nim-reranker";
  private static final String OTHER_CUSTOM_MODEL_NAME = "my-org/my-other-nim-reranker";
  private static final String NVIDIA_MODEL_NAME = "nvidia/llama-3.2-nv-rerankqa-1b-v2";

  private static final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig
          .RequestProperties
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 100, 5000, 500, 0.5, 10);

  private static RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig(
      String name, String url) {
    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
        name,
        new ApiModelSupport.ApiModelSupportImpl(
            ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
        false,
        url,
        REQUEST_PROPERTIES);
  }

  private static RerankingProvidersConfig.RerankingProviderConfig providerConfig(
      String displayName, RerankingProvidersConfig.RerankingProviderConfig.ModelConfig... models) {
    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
        false,
        displayName,
        true,
        Map.of(
            RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType.NONE,
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.AuthenticationConfigImpl(
                true, List.of())),
        List.of(models));
  }

  private static final RerankingProvidersConfig CONFIG =
      new RerankingProvidersConfigImpl(
          Map.of(
              ModelProvider.NVIDIA.apiName(),
              providerConfig(
                  "Nvidia",
                  modelConfig(
                      NVIDIA_MODEL_NAME,
                      "https://us-west-2.api-dev.ai.datastax.com/nvidia/v1/ranking")),
              ModelProvider.CUSTOM.apiName(),
              providerConfig(
                  "Custom",
                  modelConfig(CUSTOM_MODEL_NAME, "http://localhost:8000/v1/ranking"),
                  modelConfig(OTHER_CUSTOM_MODEL_NAME, "http://localhost:8001/v1/ranking"))));

  private static RerankingProviderFactory factoryWith(RerankingProvidersConfig config) {
    var factory = new RerankingProviderFactory();
    factory.rerankingConfig = config;
    factory.operationsConfig = mock(OperationsConfig.class);
    when(factory.operationsConfig.enableEmbeddingGateway()).thenReturn(false);
    return factory;
  }

  @Test
  public void customProviderCreatesNimClientWithCustomAttribution() {
    var provider =
        factoryWith(CONFIG)
            .create(
                testConstants.TENANT,
                "test-token",
                ModelProvider.CUSTOM.apiName(),
                CUSTOM_MODEL_NAME,
                null,
                "testCommand");

    assertThat(provider)
        .as("custom provider reuses the NIM wire-format client")
        .isInstanceOf(NvidiaRerankingProvider.class);
    assertThat(provider.modelProvider())
        .as("metrics/usage must attribute to 'custom', not 'nvidia'")
        .isEqualTo(ModelProvider.CUSTOM);
    assertThat(provider.modelName()).isEqualTo(CUSTOM_MODEL_NAME);
  }

  @Test
  public void nvidiaProviderAttributionUnchanged() {
    var provider =
        factoryWith(CONFIG)
            .create(
                testConstants.TENANT,
                "test-token",
                ModelProvider.NVIDIA.apiName(),
                NVIDIA_MODEL_NAME,
                null,
                "testCommand");

    assertThat(provider).isInstanceOf(NvidiaRerankingProvider.class);
    assertThat(provider.modelProvider()).isEqualTo(ModelProvider.NVIDIA);
    assertThat(provider.modelName()).isEqualTo(NVIDIA_MODEL_NAME);
  }

  @Test
  public void unknownModelForCustomProviderThrows() {
    assertThatThrownBy(
            () ->
                factoryWith(CONFIG)
                    .create(
                        testConstants.TENANT,
                        "test-token",
                        ModelProvider.CUSTOM.apiName(),
                        "no-such-model",
                        null,
                        "testCommand"))
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e ->
                assertThat(((SchemaException) e).code)
                    .isEqualTo(SchemaException.Code.RERANKING_SERVICE_TYPE_UNAVAILABLE.name()));
  }

  @Test
  public void repeatedCreateReturnsSameCachedInstance() {
    var factory = factoryWith(CONFIG);

    var first =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.CUSTOM.apiName(),
            CUSTOM_MODEL_NAME,
            null,
            "testCommand");
    var second =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.CUSTOM.apiName(),
            CUSTOM_MODEL_NAME,
            null,
            "testCommand");
    var otherTenant =
        factory.create(
            testConstants.CASSANDRA_TENANT,
            "other-token",
            ModelProvider.CUSTOM.apiName(),
            CUSTOM_MODEL_NAME,
            null,
            "otherCommand");

    assertThat(second)
        .as("same (provider, model) must reuse the provider and its HTTP connection pool")
        .isSameAs(first);
    assertThat(otherTenant)
        .as("tenant and auth are per-rerank() state, so the cached instance is tenant-agnostic")
        .isSameAs(first);
  }

  @Test
  public void differentModelsGetDistinctInstances() {
    var factory = factoryWith(CONFIG);

    var custom =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.CUSTOM.apiName(),
            CUSTOM_MODEL_NAME,
            null,
            "testCommand");
    var otherCustom =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.CUSTOM.apiName(),
            OTHER_CUSTOM_MODEL_NAME,
            null,
            "testCommand");
    var nvidia =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.NVIDIA.apiName(),
            NVIDIA_MODEL_NAME,
            null,
            "testCommand");

    assertThat(otherCustom)
        .as("different models under the same provider must not share an instance")
        .isNotSameAs(custom);
    assertThat(nvidia).isNotSameAs(custom).isNotSameAs(otherCustom);
    assertThat(otherCustom.modelName()).isEqualTo(OTHER_CUSTOM_MODEL_NAME);
  }

  @Test
  public void providerNotInConfigThrows() {
    // 'cohere' is a known ModelProvider enum value but is not registered in the reranking config
    assertThatThrownBy(
            () ->
                factoryWith(CONFIG)
                    .create(
                        testConstants.TENANT,
                        "test-token",
                        ModelProvider.COHERE.apiName(),
                        "rerank-english-v3.0",
                        null,
                        "testCommand"))
        .isInstanceOf(SchemaException.class)
        .satisfies(
            e ->
                assertThat(((SchemaException) e).code)
                    .isEqualTo(SchemaException.Code.RERANKING_SERVICE_TYPE_UNAVAILABLE.name()));
  }
}
