package io.stargate.sgv2.jsonapi.service.schema.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CollectionRerankDef#fromApiDesc} focusing on behavior when the reranking
 * feature is disabled at the API level.
 *
 * <p>Reproduces <a href="https://github.com/stargate/data-api/issues/2423">#2423</a>: when
 * reranking feature is not enabled, explicitly passing {@code "rerank": {"enabled": false}} should
 * succeed (return disabled config) instead of throwing RERANKING_FEATURE_NOT_ENABLED.
 */
class CollectionRerankDefTest {

  // Minimal stub: no providers configured
  private static final RerankingProvidersConfig EMPTY_PROVIDERS_CONFIG =
      new RerankingProvidersConfig() {
        @Override
        public Map<String, RerankingProviderConfig> providers() {
          return Map.of();
        }
      };

  @Nested
  class WhenRerankingFeatureDisabled {

    /**
     * Baseline: null rerank desc (user omits "rerank" entirely) should return disabled config —
     * this already works.
     */
    @Test
    void shouldReturnDisabledWhenNoDescProvided() {
      CollectionRerankDef result =
          CollectionRerankDef.fromApiDesc(
              false, // reranking NOT enabled
              null, // no rerank desc
              EMPTY_PROVIDERS_CONFIG);

      assertThat(result).isNotNull();
      assertThat(result.enabled()).isFalse();
    }

    /**
     * Reproduces issue #2423: user explicitly passes {"enabled": false} for rerank when the feature
     * is disabled. This should succeed but currently throws RERANKING_FEATURE_NOT_ENABLED.
     */
    @Test
    void shouldReturnDisabledWhenExplicitlyDisabled() {
      // User sends: "rerank": {"enabled": false}
      var rerankDesc = new CreateCollectionCommand.Options.RerankDesc(false, null);

      // This should NOT throw — the user is saying "I don't want reranking"
      // which matches the server state (reranking not available).
      // BUG: currently throws SchemaException RERANKING_FEATURE_NOT_ENABLED
      CollectionRerankDef result =
          CollectionRerankDef.fromApiDesc(
              false, // reranking NOT enabled
              rerankDesc,
              EMPTY_PROVIDERS_CONFIG);

      assertThat(result).isNotNull();
      assertThat(result.enabled()).isFalse();
    }

    /**
     * When the feature is disabled, user trying to ENABLE reranking should still fail with
     * RERANKING_FEATURE_NOT_ENABLED.
     */
    @Test
    void shouldFailWhenTryingToEnableReranking() {
      // User sends: "rerank": {"enabled": true, "service": {...}}
      var rerankDesc = new CreateCollectionCommand.Options.RerankDesc(true, null);

      assertThatThrownBy(
              () ->
                  CollectionRerankDef.fromApiDesc(
                      false, // reranking NOT enabled
                      rerankDesc,
                      EMPTY_PROVIDERS_CONFIG))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue(
              "code", SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.name());
    }
  }
}
