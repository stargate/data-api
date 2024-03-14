package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingProviderConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Operation that list all available vector providers into the {@link
 * CommandStatus#EXISTING_VECTOR_PROVIDERS} command status.
 */
public record FindEmbeddingProvidersOperation(
    PropertyBasedEmbeddingProviderConfig propertyBasedEmbeddingProviderConfig)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, EmbeddingProviderResponse> embeddingProviders = new HashMap<>();
              if (propertyBasedEmbeddingProviderConfig.providers().get("vertexai").enabled()) {
                embeddingProviders.put(
                    "vertexai",
                    EmbeddingProviderResponse.provider(
                        (propertyBasedEmbeddingProviderConfig.providers().get("vertexai"))));
              }
              if (propertyBasedEmbeddingProviderConfig.providers().get("huggingface").enabled()) {
                embeddingProviders.put(
                    "huggingface",
                    EmbeddingProviderResponse.provider(
                        (propertyBasedEmbeddingProviderConfig.providers().get("huggingface"))));
              }
              if (propertyBasedEmbeddingProviderConfig.providers().get("openai").enabled()) {
                embeddingProviders.put(
                    "openai",
                    EmbeddingProviderResponse.provider(
                        (propertyBasedEmbeddingProviderConfig.providers().get("openai"))));
              }
              if (propertyBasedEmbeddingProviderConfig.providers().get("cohere").enabled()) {
                embeddingProviders.put(
                    "cohere",
                    EmbeddingProviderResponse.provider(
                        (propertyBasedEmbeddingProviderConfig.providers().get("cohere"))));
              }
              if (propertyBasedEmbeddingProviderConfig.providers().get("nvidia").enabled()) {
                embeddingProviders.put(
                    "nvidia",
                    EmbeddingProviderResponse.provider(
                        (propertyBasedEmbeddingProviderConfig.providers().get("nvidia"))));
              }
              return new Result(embeddingProviders);
            });
  }

  // simple result wrapper
  private record Result(Map<String, EmbeddingProviderResponse> embeddingProviders)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses =
          Map.of(CommandStatus.EXISTING_VECTOR_PROVIDERS, embeddingProviders);
      return new CommandResult(statuses);
    }
  }

  /**
   * A simplified representation of a vector provider's configuration for API responses. Excludes
   * internal properties (retry, timeout etc.) to focus on data relevant to clients, including URL,
   * authentication methods, and model customization parameters.
   *
   * @param url URL of the vector provider's service.
   * @param supportedAuthentication Supported methods for authentication.
   * @param parameters Customizable parameters for the provider's service.
   * @param models Model configurations available from the provider.
   */
  private record EmbeddingProviderResponse(
      String url,
      List<String> supportedAuthentication,
      List<PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterConfig> parameters,
      List<ModelConfigResponse> models) {
    private static EmbeddingProviderResponse provider(
        PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig embeddingProviderConfig) {
      ArrayList<ModelConfigResponse> modelsRemoveProperties = new ArrayList<>();
      for (PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ModelConfig model :
          embeddingProviderConfig.models()) {
        ModelConfigResponse returnModel =
            new ModelConfigResponse(model.name(), model.vectorDimension(), model.parameters());
        modelsRemoveProperties.add(returnModel);
      }
      return new EmbeddingProviderResponse(
          embeddingProviderConfig.url(),
          embeddingProviderConfig.supportedAuthentication(),
          embeddingProviderConfig.parameters(),
          modelsRemoveProperties);
    }
  }

  /**
   * Configuration details for a model offered by a vector provider, tailored for external clients.
   * Includes the model name and parameters for customization, excluding internal properties (retry,
   * timeout etc.).
   *
   * @param name Identifier name of the model.
   * @param vectorDimension vector dimension of the model.
   * @param parameters Parameters for customizing the model.
   */
  private record ModelConfigResponse(
      String name,
      Integer vectorDimension,
      List<PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterConfig>
          parameters) {}
}
