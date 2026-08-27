package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.stargate.embedding.gateway.RerankingService;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RerankingProviderFactoryTest {

  private static final String MODEL_NAME = "nvidia/test-model";
  private static final Tenant TENANT = Tenant.create(DatabaseType.ASTRA, "test-tenant");

  private final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig =
      mock(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.class);
  private final RerankingProvidersConfig.RerankingProviderConfig providerConfig =
      mock(RerankingProvidersConfig.RerankingProviderConfig.class);
  private final RerankingProvidersConfig rerankingConfig = mock(RerankingProvidersConfig.class);
  private final OperationsConfig operationsConfig = mock(OperationsConfig.class);

  @BeforeEach
  void setUp() {
    var requestProperties =
        mock(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties.class);
    when(requestProperties.initialBackOffMillis()).thenReturn(100);
    when(requestProperties.maxBackOffMillis()).thenReturn(500);
    when(modelConfig.name()).thenReturn(MODEL_NAME);
    when(modelConfig.properties()).thenReturn(requestProperties);
    when(providerConfig.models()).thenReturn(List.of(modelConfig));
    when(rerankingConfig.providers())
        .thenReturn(Map.of(ModelProvider.NVIDIA.apiName(), providerConfig));
  }

  @Test
  void reusesDirectProviderAcrossRequests() {
    var creations = new AtomicInteger();
    var directProvider = mock(RerankingProvider.class);
    var factory =
        factory(
            config -> {
              creations.incrementAndGet();
              return directProvider;
            });

    var first = create(factory, TENANT, "first-token");
    var second =
        create(factory, Tenant.create(DatabaseType.ASTRA, "another-tenant"), "second-token");

    assertThat(first).isSameAs(second);
    assertThat(creations).hasValue(1);
  }

  @Test
  void createsOneDirectProviderUnderConcurrency() throws Exception {
    var creations = new AtomicInteger();
    var factory =
        factory(
            config -> {
              creations.incrementAndGet();
              return mock(RerankingProvider.class);
            });

    try (var executor = Executors.newFixedThreadPool(16)) {
      var requests =
          java.util.stream.IntStream.range(0, 64)
              .<java.util.concurrent.Callable<RerankingProvider>>mapToObj(
                  index -> () -> create(factory, TENANT, "token-" + index))
              .toList();

      var providers =
          executor.invokeAll(requests).stream()
              .map(
                  future -> {
                    try {
                      return future.get();
                    } catch (Exception exception) {
                      throw new AssertionError(exception);
                    }
                  })
              .toList();

      assertThat(providers).allMatch(provider -> provider == providers.getFirst());
      assertThat(creations).hasValue(1);
    }
  }

  @Test
  void doesNotShareDirectProvidersAcrossModels() {
    var otherModel = mock(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.class);
    when(otherModel.name()).thenReturn("nvidia/other-model");
    when(providerConfig.models()).thenReturn(List.of(modelConfig, otherModel));
    var factory = factory(config -> mock(RerankingProvider.class));

    var first = create(factory, TENANT, "token");
    var second =
        factory.create(
            TENANT,
            "token",
            ModelProvider.NVIDIA.apiName(),
            otherModel.name(),
            Map.of(),
            "findAndRerank");

    assertThat(first).isNotSameAs(second);
  }

  @Test
  void keepsGatewayProvidersRequestScoped() {
    when(operationsConfig.enableEmbeddingGateway()).thenReturn(true);
    var factory = factory(config -> mock(RerankingProvider.class));
    factory.grpcGatewayService = mock(RerankingService.class);

    var first = create(factory, TENANT, "first-token");
    var second = create(factory, TENANT, "second-token");

    assertThat(first).isNotSameAs(second);
    assertThat(first)
        .isInstanceOf(io.stargate.sgv2.jsonapi.service.reranking.gateway.RerankingEGWClient.class);
    assertThat(second)
        .isInstanceOf(io.stargate.sgv2.jsonapi.service.reranking.gateway.RerankingEGWClient.class);
  }

  @Test
  void closesCachedDirectProvidersAtShutdown() throws Exception {
    var directProvider =
        mock(RerankingProvider.class, withSettings().extraInterfaces(AutoCloseable.class));
    var factory = factory(config -> directProvider);
    create(factory, TENANT, "token");

    factory.close();

    verify((AutoCloseable) directProvider).close();
  }

  private RerankingProviderFactory factory(
      RerankingProviderFactory.ProviderConstructor providerConstructor) {
    var factory = new RerankingProviderFactory(Map.of(ModelProvider.NVIDIA, providerConstructor));
    factory.rerankingConfig = rerankingConfig;
    factory.operationsConfig = operationsConfig;
    return factory;
  }

  private RerankingProvider create(
      RerankingProviderFactory factory, Tenant tenant, String authToken) {
    return factory.create(
        tenant, authToken, ModelProvider.NVIDIA.apiName(), MODEL_NAME, Map.of(), "findAndRerank");
  }
}
