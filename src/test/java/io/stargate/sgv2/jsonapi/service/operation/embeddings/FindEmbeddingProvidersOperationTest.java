package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.operation.collections.OperationTestBase;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindEmbeddingProvidersOperationTest extends OperationTestBase {
  @Inject ObjectMapper objectMapper;

  @Test
  public void testDoNotReturnDeprecatedModels() {
    CommandResult result = executeFindEmbeddingProvidersOperation(false);
    Map<String, FindEmbeddingProvidersOperation.EmbeddingProviderResponse> configMap =
        (Map<String, FindEmbeddingProvidersOperation.EmbeddingProviderResponse>)
            result.status().get(CommandStatus.EXISTING_VECTOR_PROVIDERS);
    assertThat(configMap).hasSize(1);
    FindEmbeddingProvidersOperation.EmbeddingProviderResponse providerConfig =
        configMap.get("provider1");
    assertThat(providerConfig.models()).hasSize(1);
    assertThat(providerConfig.models().getFirst().name()).isEqualTo("supported-model");
  }

  @Test
  public void testReturnDeprecatedModels() {
    CommandResult result = executeFindEmbeddingProvidersOperation(true);
    Map<String, FindEmbeddingProvidersOperation.EmbeddingProviderResponse> configMap =
        (Map<String, FindEmbeddingProvidersOperation.EmbeddingProviderResponse>)
            result.status().get(CommandStatus.EXISTING_VECTOR_PROVIDERS);
    assertThat(configMap).hasSize(1);
    FindEmbeddingProvidersOperation.EmbeddingProviderResponse providerConfig =
        configMap.get("provider1");
    assertThat(providerConfig.models()).hasSize(2);
    assertThat(providerConfig.models().getFirst().name()).isEqualTo("supported-model");
    assertThat(providerConfig.models().getLast().name()).isEqualTo("deprecated-model");
  }

  private CommandResult executeFindEmbeddingProvidersOperation(boolean returnDeprecatedModels) {
    FindEmbeddingProvidersOperation findEmbeddingProvidersOperation =
        new FindEmbeddingProvidersOperation(
            getTestEmbeddingProvidersConfig(), returnDeprecatedModels);
    Supplier<CommandResult> execute =
        findEmbeddingProvidersOperation
            .execute(null, null)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
    return execute.get();
  }

  private EmbeddingProvidersConfig getTestEmbeddingProvidersConfig() {
    return new EmbeddingProvidersConfig() {
      @Override
      public Map<String, EmbeddingProviderConfig> providers() {
        List<EmbeddingProviderConfig.ModelConfig> modelConfigs =
            List.of(
                new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
                    "supported-model",
                    Optional.of(1),
                    List.of(),
                    Map.of(),
                    Optional.of(false),
                    Optional.of("url1")),
                new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
                    "deprecated-model",
                    Optional.of(1),
                    List.of(),
                    Map.of(),
                    Optional.of(true),
                    Optional.of("url1")));
        EmbeddingProviderConfig providerConfig =
            new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
                "provider1", true, Optional.of("url1"), Map.of(), List.of(), null, modelConfigs);
        return Map.of("provider1", providerConfig);
      }

      @Override
      public @Nullable CustomConfig custom() {
        return null;
      }
    };
  }
}
