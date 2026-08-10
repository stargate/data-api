package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.syncservice.SyncServiceClient;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EmbeddingProviderFactory}, in particular that direct-mode (non-gateway)
 * providers are cached and reused across commands so their REST clients keep one HTTP connection
 * pool, mirroring the equivalent RerankingProviderFactory caching. The cache key is (provider,
 * model, dimension, vectorize service parameters) because provider constructors consume all four,
 * while credentials and tenant arrive per {@code vectorize()} call.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class EmbeddingProviderFactoryTest {

  private static final TestConstants testConstants = new TestConstants();

  private static final String NVIDIA_MODEL_NAME = "nvidia/nv-embedqa-e5-v5";
  private static final String OTHER_NVIDIA_MODEL_NAME = "nvidia/llama-3.2-nv-embedqa-1b-v2";

  private static final EmbeddingProvidersConfig.EmbeddingProviderConfig.RequestProperties
      REQUEST_PROPERTIES =
          new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
              3, 100, 5000, 500, 0.5, Optional.empty(), Optional.empty(), Optional.empty(), 10);

  private static EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig(
      String name) {
    return new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
        name,
        new ApiModelSupport.ApiModelSupportImpl(
            ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
        Optional.empty(),
        List.of(),
        Map.of(),
        Optional.empty());
  }

  private static final EmbeddingProvidersConfig CONFIG =
      new EmbeddingProvidersConfigImpl(
          Map.of(
              ModelProvider.NVIDIA.apiName(),
              new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
                  "Nvidia",
                  true,
                  Optional.of("http://localhost:8000/v1/embeddings"),
                  false,
                  Map.of(),
                  List.of(),
                  REQUEST_PROPERTIES,
                  List.of(modelConfig(NVIDIA_MODEL_NAME), modelConfig(OTHER_NVIDIA_MODEL_NAME)))),
          null);

  private static final ServiceConfigStore.ServiceConfig NVIDIA_SERVICE_CONFIG =
      ServiceConfigStore.ServiceConfig.forKnownProvider(
          ModelProvider.NVIDIA,
          "http://localhost:8000/v1/embeddings",
          new ServiceConfigStore.ServiceRequestProperties(
              3, 100, 5000, 500, 0.5, Optional.empty(), Optional.empty(), 10),
          Map.of());

  private static EmbeddingProviderFactory factoryWith(
      ServiceConfigStore.ServiceConfig serviceConfig) {
    var factory = new EmbeddingProviderFactory();
    factory.embeddingProvidersConfig = CONFIG;

    @SuppressWarnings("unchecked")
    Instance<ServiceConfigStore> storeInstance = mock(Instance.class);
    when(storeInstance.get()).thenReturn(modelProvider -> serviceConfig);
    factory.embeddingProviderConfigStore = storeInstance;

    factory.operationsConfig = mock(OperationsConfig.class);
    when(factory.operationsConfig.enableEmbeddingGateway()).thenReturn(false);

    factory.syncServiceClient = mock(SyncServiceClient.class);
    return factory;
  }

  private static EmbeddingProvider create(
      EmbeddingProviderFactory factory,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      Map<String, String> authentication) {
    return factory.create(
        testConstants.TENANT,
        "test-token",
        ModelProvider.NVIDIA.apiName(),
        modelName,
        dimension,
        vectorizeServiceParameters,
        authentication,
        "testCommand");
  }

  @Test
  public void repeatedCreateReturnsSameCachedInstance() {
    var factory = factoryWith(NVIDIA_SERVICE_CONFIG);

    var first = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of("k", "v"), null);
    var second = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of("k", "v"), null);
    var otherTenant =
        factory.create(
            testConstants.CASSANDRA_TENANT,
            "other-token",
            ModelProvider.NVIDIA.apiName(),
            NVIDIA_MODEL_NAME,
            1024,
            Map.of("k", "v"),
            null,
            "otherCommand");

    assertThat(second)
        .as(
            "same (provider, model, dimension, parameters) must reuse the provider and its HTTP connection pool")
        .isSameAs(first);
    assertThat(otherTenant)
        .as("tenant and auth are per-vectorize() state, so the cached instance is tenant-agnostic")
        .isSameAs(first);
  }

  @Test
  public void nullAndEmptyParametersShareTheSameCacheEntry() {
    var factory = factoryWith(NVIDIA_SERVICE_CONFIG);

    var nullParams = create(factory, NVIDIA_MODEL_NAME, 1024, null, null);
    var emptyParams = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of(), null);

    assertThat(emptyParams)
        .as("null parameters are normalized to an empty map before keying the cache")
        .isSameAs(nullParams);
  }

  @Test
  public void differentKeyComponentsGetDistinctInstances() {
    var factory = factoryWith(NVIDIA_SERVICE_CONFIG);

    var base = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of(), null);
    var differentModel = create(factory, OTHER_NVIDIA_MODEL_NAME, 1024, Map.of(), null);
    var differentDimension = create(factory, NVIDIA_MODEL_NAME, 512, Map.of(), null);
    var differentParameters =
        create(factory, NVIDIA_MODEL_NAME, 1024, Map.of("autoTruncate", true), null);

    assertThat(differentModel)
        .as("different models under the same provider must not share an instance")
        .isNotSameAs(base);
    assertThat(differentModel.modelName()).isEqualTo(OTHER_NVIDIA_MODEL_NAME);
    assertThat(differentDimension)
        .as("dimension is consumed by provider constructors, so it is part of the cache key")
        .isNotSameAs(base);
    assertThat(differentParameters)
        .as(
            "vectorize service parameters are consumed by provider constructors (e.g. URL placeholders), so they are part of the cache key")
        .isNotSameAs(base);
  }

  @Test
  public void sharedSecretAuthenticationWrapsCachedProviderPerCall() {
    var factory = factoryWith(NVIDIA_SERVICE_CONFIG);
    Map<String, String> authentication = Map.of("providerKey", "my-cred");

    var wrappedFirst = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of(), authentication);
    var wrappedSecond = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of(), authentication);
    var bare = create(factory, NVIDIA_MODEL_NAME, 1024, Map.of(), null);

    assertThat(wrappedFirst).isInstanceOf(SyncServiceCredentialResolvingProvider.class);
    assertThat(wrappedSecond)
        .as("the wrapper carries per-request tenant/auth state and must never be cached")
        .isNotSameAs(wrappedFirst);
    assertThat(((SyncServiceCredentialResolvingProvider) wrappedFirst).delegate())
        .as("the wrapped inner provider must still come from the cache")
        .isSameAs(bare)
        .isSameAs(((SyncServiceCredentialResolvingProvider) wrappedSecond).delegate());
  }

  @Test
  public void customReflectionProviderIsNotCached() {
    var factory =
        factoryWith(
            ServiceConfigStore.ServiceConfig.forCustomProvider(CustomITEmbeddingProvider.class));

    var first =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.CUSTOM.apiName(),
            "custom-model",
            3,
            null,
            null,
            "testCommand");
    var second =
        factory.create(
            testConstants.TENANT,
            "test-token",
            ModelProvider.CUSTOM.apiName(),
            "custom-model",
            3,
            null,
            null,
            "testCommand");

    assertThat(first).isInstanceOf(CustomITEmbeddingProvider.class);
    assertThat(second).as("the test-only CUSTOM reflection path stays uncached").isNotSameAs(first);
  }
}
