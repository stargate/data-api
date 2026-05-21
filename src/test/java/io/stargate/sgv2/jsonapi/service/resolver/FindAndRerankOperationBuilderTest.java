package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.Billing;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderTest {

  // @QuarkusTest needed for error template initialization
  @InjectMock protected RequestContext dataApiRequestInfo;

  @Inject ObjectMapper objectMapper;
  @Inject FindCommandResolver findCommandResolver;

  private final TestConstants testConstants = new TestConstants();

  // Reusable request properties for model configs
  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10);

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

  /** Helper that calls the centralized validation with the override error code. */
  private static void validateOverride(
      RerankingProvidersConfig config, String provider, String modelName) {
    CollectionRerankDef.validateServiceDesc(
        config, provider, modelName, null, null, RequestException.Code.INVALID_RERANK_OVERRIDE);
  }

  @Test
  void keepsExplicitHybridLimitsAtMaximumPageSizeOnCommandContext() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": { "$vector": 100, "$lexical": 25 }
                }
              }
            }
            """);

    new FindAndRerankOperationBuilder(commandContext, mock(Billing.class))
        .withCommand(command)
        .withFindCommandResolver(findCommandResolver)
        .build();

    assertThat(commandContext.getHybridLimits().vectorLimit())
        .isEqualTo(RerankingConstants.HybridSearchLimits.MAX);
    assertThat(commandContext.getHybridLimits().lexicalLimit()).isEqualTo(25);
  }

  @Test
  void failsWhenVectorLimitAboveConfiguredMax() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": { "$vector": 101, "$lexical": 50 }
                }
              }
            }
            """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext, mock(Billing.class))
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining("hybridLimits.$vector")
        .hasMessageContaining("101")
        .hasMessageContaining("must be between 1 and 100");
  }

  @Test
  void failsWhenLexicalLimitAboveConfiguredMax() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": { "$vector": 50, "$lexical": 101 }
                }
              }
            }
            """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext, mock(Billing.class))
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining("hybridLimits.$lexical")
        .hasMessageContaining("101")
        .hasMessageContaining("must be between 1 and 100");
  }

  @Test
  void failsWhenLimitBelowConfiguredMin() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": 0
                }
              }
            }
            """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext, mock(Billing.class))
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining("hybridLimits.$vector")
        .hasMessageContaining("must be between 1 and 100");
  }

  @Test
  void acceptsBoundaryValues() throws Exception {
    var commandContextLow = commandContext();
    var commandLow =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": 1
                }
              }
            }
            """);

    new FindAndRerankOperationBuilder(commandContextLow, mock(Billing.class))
        .withCommand(commandLow)
        .withFindCommandResolver(findCommandResolver)
        .build();

    var commandContextHigh = commandContext();
    var commandHigh =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": 100
                }
              }
            }
            """);

    new FindAndRerankOperationBuilder(commandContextHigh, mock(Billing.class))
        .withCommand(commandHigh)
        .withFindCommandResolver(findCommandResolver)
        .build();
  }

  private FindAndRerankCommand command(String json) throws Exception {
    return objectMapper.readValue(json, FindAndRerankCommand.class);
  }

  private CommandContext<CollectionSchemaObject> commandContext() {
    var commandContext =
        testConstants.collectionContext(
            CommandName.FIND_AND_RERANK,
            testConstants.VECTOR_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT);

    var rerankingProvidersConfig = mock(RerankingProvidersConfig.class);
    var modelConfig = mock(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.class);
    when(modelConfig.apiModelSupport())
        .thenReturn(
            new ApiModelSupport.ApiModelSupportImpl(
                ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()));
    when(rerankingProvidersConfig.filterByRerankServiceDef(any())).thenReturn(modelConfig);
    when(commandContext.rerankingProviderFactory().getRerankingConfig())
        .thenReturn(rerankingProvidersConfig);
    when(commandContext.rerankingProviderFactory().create(any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(RerankingProvider.class));

    return commandContext;
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
      assertThatCode(() -> validateOverride(NVIDIA_SUPPORTED, "nvidia", "nvidia/rerank-v1"))
          .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectUnknownProvider() {
      assertThatThrownBy(() -> validateOverride(NVIDIA_SUPPORTED, "unknown-provider", "some-model"))
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

      assertThatThrownBy(() -> validateOverride(disabledConfig, "nvidia", "nvidia/rerank-v1"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }

    @Test
    void shouldRejectNullModelName() {
      assertThatThrownBy(() -> validateOverride(NVIDIA_SUPPORTED, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("Model name is required");
    }

    @Test
    void shouldRejectUnknownModel() {
      assertThatThrownBy(
              () -> validateOverride(NVIDIA_SUPPORTED, "nvidia", "nvidia/nonexistent-model"))
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

      assertThatThrownBy(() -> validateOverride(config, "nvidia", "nvidia/old-model"))
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

      assertThatThrownBy(() -> validateOverride(config, "nvidia", "nvidia/eol-model"))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue("code", SchemaException.Code.END_OF_LIFE_AI_MODEL.name());
    }

    @Test
    void shouldRejectUnknownProviderBeforeCheckingModelName() {
      // When both provider is unknown AND modelName is null, the provider check should
      // come first — user gets the more actionable "provider not supported" error
      assertThatThrownBy(() -> validateOverride(NVIDIA_SUPPORTED, "unknown-provider", null))
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

      assertThatThrownBy(() -> validateOverride(disabledConfig, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }
  }
}
