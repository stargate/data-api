package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderTest {

  @InjectMock protected RequestContext dataApiRequestInfo;

  private final TestConstants testConstants = new TestConstants();

  private FindAndRerankOperationBuilder builder;

  // Reusable request properties for model configs
  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10);

  @BeforeEach
  void beforeEach() {
    CommandContext<CollectionSchemaObject> commandContext = testConstants.collectionContext();
    builder = new FindAndRerankOperationBuilder(commandContext);
  }

  private static RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig(
      String name, ApiModelSupport.SupportStatus status) {
    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
        name,
        new ApiModelSupport.ApiModelSupportImpl(status, Optional.empty()),
        false,
        "https://example.com/rerank",
        REQUEST_PROPERTIES);
  }

  private static RerankingProvidersConfig configWithProvider(
      String providerName,
      boolean enabled,
      List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models) {
    return new RerankingProvidersConfigImpl(
        Map.of(
            providerName,
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
                false, providerName, enabled, Map.of(), models)));
  }

  @Nested
  class ValidateRerankOverride {

    // Shared config: nvidia provider enabled with a single supported model
    private final RerankingProvidersConfig NVIDIA_SUPPORTED =
        configWithProvider(
            "nvidia",
            true,
            List.of(modelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED)));

    @Test
    void shouldAcceptSupportedProviderAndModel() {
      assertThatCode(
              () -> builder.validateRerankOverride(NVIDIA_SUPPORTED, "nvidia", "nvidia/rerank-v1"))
          .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectUnknownProvider() {
      assertThatThrownBy(
              () ->
                  builder.validateRerankOverride(
                      NVIDIA_SUPPORTED, "unknown-provider", "some-model"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("unknown-provider");
    }

    @Test
    void shouldRejectDisabledProvider() {
      var disabledConfig =
          configWithProvider(
              "nvidia",
              false,
              List.of(modelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED)));

      assertThatThrownBy(
              () -> builder.validateRerankOverride(disabledConfig, "nvidia", "nvidia/rerank-v1"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }

    @Test
    void shouldRejectNullModelName() {
      assertThatThrownBy(() -> builder.validateRerankOverride(NVIDIA_SUPPORTED, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("modelName");
    }

    @Test
    void shouldRejectUnknownModel() {
      assertThatThrownBy(
              () ->
                  builder.validateRerankOverride(
                      NVIDIA_SUPPORTED, "nvidia", "nvidia/nonexistent-model"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("nonexistent-model");
    }

    @Test
    void shouldRejectDeprecatedModel() {
      var config =
          configWithProvider(
              "nvidia",
              true,
              List.of(modelConfig("nvidia/old-model", ApiModelSupport.SupportStatus.DEPRECATED)));

      assertThatThrownBy(() -> builder.validateRerankOverride(config, "nvidia", "nvidia/old-model"))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue("code", SchemaException.Code.DEPRECATED_AI_MODEL.name());
    }

    @Test
    void shouldRejectEndOfLifeModel() {
      var config =
          configWithProvider(
              "nvidia",
              true,
              List.of(modelConfig("nvidia/eol-model", ApiModelSupport.SupportStatus.END_OF_LIFE)));

      assertThatThrownBy(() -> builder.validateRerankOverride(config, "nvidia", "nvidia/eol-model"))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue("code", SchemaException.Code.END_OF_LIFE_AI_MODEL.name());
    }

    @Test
    void shouldRejectUnknownProviderBeforeCheckingModelName() {
      // When both provider is unknown AND modelName is null, the provider check should
      // come first — user gets the more actionable "provider not supported" error
      assertThatThrownBy(
              () -> builder.validateRerankOverride(NVIDIA_SUPPORTED, "unknown-provider", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("unknown-provider")
          .hasMessageContaining("not supported");
    }

    @Test
    void shouldRejectDisabledProviderBeforeCheckingModelName() {
      // When provider is disabled AND modelName is null, the disabled check should
      // come first — user gets "provider disabled" instead of "modelName required"
      var disabledConfig =
          configWithProvider(
              "nvidia",
              false,
              List.of(modelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED)));

      assertThatThrownBy(() -> builder.validateRerankOverride(disabledConfig, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }
  }
}
