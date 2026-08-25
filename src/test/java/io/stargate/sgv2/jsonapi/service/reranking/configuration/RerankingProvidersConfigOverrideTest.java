package io.stargate.sgv2.jsonapi.service.reranking.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Proves the concurrency-gate properties are tunable per deployment through standard config
 * overrides (system properties / env vars / chart values), not only through the bundled yaml.
 */
@QuarkusTest
@TestProfile(RerankingProvidersConfigOverrideTest.Profile.class)
public class RerankingProvidersConfigOverrideTest {

  public static class Profile implements QuarkusTestProfile {
    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put(
              "stargate.jsonapi.reranking.providers.nvidia.models[0].properties.max-concurrent-batches",
              "2")
          .put(
              "stargate.jsonapi.reranking.providers.nvidia.models[0].properties.max-concurrent-calls",
              "5")
          .put(
              "stargate.jsonapi.reranking.providers.nvidia.models[0].properties.max-queued-calls",
              "7")
          .put(
              "stargate.jsonapi.reranking.providers.nvidia.models[0].properties.total-timeout-millis",
              "12345")
          .build();
    }
  }

  @Inject RerankingProvidersConfig config;

  @Test
  @DisplayName("concurrency gate properties are overridable via standard config keys")
  void overridesApply() {
    var properties = config.providers().get("nvidia").models().get(0).properties();

    assertThat(properties.maxConcurrentBatches()).isEqualTo(2);
    assertThat(properties.maxConcurrentCalls()).isEqualTo(5);
    assertThat(properties.maxQueuedCalls()).isEqualTo(7);
    assertThat(properties.totalTimeoutMillis()).isEqualTo(12345);
  }
}
