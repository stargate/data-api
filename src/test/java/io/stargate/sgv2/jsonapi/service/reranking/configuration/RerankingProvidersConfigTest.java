package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Asserts the reranking provider configuration loaded from the main {@code
 * reranking-providers-config.yaml} (unit tests resolve against the main yaml, not the
 * integration-test one), in particular the concurrency-gate defaults introduced for the reranking
 * bulkhead.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class RerankingProvidersConfigTest {

  private static final String RERANKQA_MODEL = "nvidia/llama-3.2-nv-rerankqa-1b-v2";
  private static final String NEMOTRON_MODEL = "nvidia/llama-3.2-nemoretriever-500m-rerank-v2";

  @Inject RerankingProvidersConfig config;

  private RerankingProvidersConfig.RerankingProviderConfig.ModelConfig model(String name) {
    return config.providers().get("nvidia").models().stream()
        .filter(model -> model.name().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("model not found in config: " + name));
  }

  @Test
  @DisplayName("concurrency gate properties default from @WithDefault when absent in yaml")
  void concurrencyGateDefaultsApplied() {
    var properties = model(RERANKQA_MODEL).properties();

    assertThat(properties.maxBatchSize()).isEqualTo(10);
    assertThat(properties.maxConcurrentBatches()).isEqualTo(8);
    assertThat(properties.maxConcurrentCalls()).isEqualTo(32);
    assertThat(properties.maxQueuedCalls()).isEqualTo(1000);
    assertThat(properties.totalTimeoutMillis()).isEqualTo(30000);
  }

  @Test
  @DisplayName("nemotron model reranks 512 passages per call to collapse the fan-out")
  void nemotronModelHasLargeBatchSize() {
    var nemotron = model(NEMOTRON_MODEL);

    assertThat(nemotron.isDefault()).isFalse();
    assertThat(nemotron.properties().maxBatchSize()).isEqualTo(512);
    // gate defaults apply to the new model too
    assertThat(nemotron.properties().maxConcurrentCalls()).isEqualTo(32);
    assertThat(nemotron.properties().totalTimeoutMillis()).isEqualTo(30000);
  }

  @Test
  @DisplayName("the legacy model stays the default model")
  void legacyModelStaysDefault() {
    assertThat(model(RERANKQA_MODEL).isDefault()).isTrue();
  }
}
