package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.embedding.gateway.EmbeddingGateway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests the mapping of the embedding gateway's reranking provider config onto {@link
 * RerankingProvidersConfig}, in particular the fallback for the concurrency-gate properties: a
 * gateway that predates proto fields 7-10 must yield the Data API defaults, never zero — zero
 * concurrent calls would deadlock every rerank request on EGW-enabled (Astra) deployments.
 */
public class RerankingProviderConfigProducerTest {

  private static final String MODEL_NAME = "nvidia/llama-3.2-nv-rerankqa-1b-v2";

  private final RerankingProviderConfigProducer producer = new RerankingProviderConfigProducer();

  private EmbeddingGateway.GetSupportedRerankingProvidersResponse responseWithProperties(
      EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig.ModelConfig
              .RequestProperties
          properties) {
    var model =
        EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig.ModelConfig
            .newBuilder()
            .setName(MODEL_NAME)
            .setApiModelSupport(
                EmbeddingGateway.ApiModelSupport.newBuilder().setStatus("SUPPORTED").build())
            .setIsDefault(true)
            .setUrl("http://testing.com")
            .setProperties(properties)
            .build();
    var provider =
        EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig.newBuilder()
            .setIsDefault(true)
            .setDisplayName("Nvidia")
            .setEnabled(true)
            .addModels(model)
            .build();
    return EmbeddingGateway.GetSupportedRerankingProvidersResponse.newBuilder()
        .putSupportedProviders("nvidia", provider)
        .build();
  }

  private RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties mapped(
      EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig.ModelConfig
              .RequestProperties
          properties) {
    var config = producer.grpcResponseToConfig(responseWithProperties(properties));
    return config.providers().get("nvidia").models().get(0).properties();
  }

  private static EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig.ModelConfig
          .RequestProperties.Builder
      baseProperties() {
    return EmbeddingGateway.GetSupportedRerankingProvidersResponse.ProviderConfig.ModelConfig
        .RequestProperties.newBuilder()
        .setAtMostRetries(3)
        .setInitialBackOffMillis(100)
        .setReadTimeoutMillis(5000)
        .setMaxBackOffMillis(500)
        .setJitter(0.5)
        .setMaxBatchSize(10);
  }

  @Test
  @DisplayName("a gateway that does not serve the gate properties yields the defaults, not zero")
  void unservedConcurrencyPropertiesFallBackToDefaults() {
    var properties = mapped(baseProperties().build());

    assertThat(properties.maxBatchSize()).isEqualTo(10);
    assertThat(properties.maxConcurrentBatches()).isEqualTo(8);
    assertThat(properties.maxConcurrentCalls()).isEqualTo(32);
    assertThat(properties.maxQueuedCalls()).isEqualTo(1000);
    assertThat(properties.totalTimeoutMillis()).isEqualTo(30000);
  }

  @Test
  @DisplayName("gateway-served gate properties pass through unchanged")
  void servedConcurrencyPropertiesPassThrough() {
    var properties =
        mapped(
            baseProperties()
                .setMaxConcurrentBatches(4)
                .setMaxConcurrentCalls(16)
                .setMaxQueuedCalls(50)
                .setTotalTimeoutMillis(20000)
                .build());

    assertThat(properties.maxConcurrentBatches()).isEqualTo(4);
    assertThat(properties.maxConcurrentCalls()).isEqualTo(16);
    assertThat(properties.maxQueuedCalls()).isEqualTo(50);
    assertThat(properties.totalTimeoutMillis()).isEqualTo(20000);
  }

  @Test
  @DisplayName("out-of-range served values fall back to defaults; explicit zero queue is honored")
  void outOfRangeServedValuesFallBackToDefaults() {
    var properties =
        mapped(
            baseProperties()
                .setMaxConcurrentBatches(0)
                .setMaxConcurrentCalls(0)
                .setMaxQueuedCalls(0)
                .setTotalTimeoutMillis(0)
                .build());

    // zero permits or a zero deadline would break every request: fall back
    assertThat(properties.maxConcurrentBatches()).isEqualTo(8);
    assertThat(properties.maxConcurrentCalls()).isEqualTo(32);
    assertThat(properties.totalTimeoutMillis()).isEqualTo(30000);
    // zero queued calls is a valid explicit "fail fast, no queueing"
    assertThat(properties.maxQueuedCalls()).isZero();
  }
}
