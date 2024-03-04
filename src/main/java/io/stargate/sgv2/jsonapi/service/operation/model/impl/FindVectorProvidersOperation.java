package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingServiceConfig;
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
public record FindVectorProvidersOperation(
    PropertyBasedEmbeddingServiceConfig propertyBasedEmbeddingServiceConfig) implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, VectorProviderResponse> vectorProviders = new HashMap<>();
              if (propertyBasedEmbeddingServiceConfig.vertexai().enabled()) {
                vectorProviders.put(
                    "vertexai",
                    VectorProviderResponse.provider(
                        (propertyBasedEmbeddingServiceConfig.vertexai())));
              }
              if (propertyBasedEmbeddingServiceConfig.huggingface().enabled()) {
                vectorProviders.put(
                    "huggingface",
                    VectorProviderResponse.provider(
                        (propertyBasedEmbeddingServiceConfig.huggingface())));
              }
              if (propertyBasedEmbeddingServiceConfig.openai().enabled()) {
                vectorProviders.put(
                    "openai",
                    VectorProviderResponse.provider(
                        (propertyBasedEmbeddingServiceConfig.openai())));
              }
              if (propertyBasedEmbeddingServiceConfig.cohere().enabled()) {
                vectorProviders.put(
                    "cohere",
                    VectorProviderResponse.provider(
                        (propertyBasedEmbeddingServiceConfig.cohere())));
              }
              if (propertyBasedEmbeddingServiceConfig.nvidia().enabled()) {
                vectorProviders.put(
                    "nvidia",
                    VectorProviderResponse.provider(
                        (propertyBasedEmbeddingServiceConfig.nvidia())));
              }
              return new Result(vectorProviders);
            });
  }

  // simple result wrapper
  private record Result(Map<String, VectorProviderResponse> vectorProviders)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses =
          Map.of(CommandStatus.EXISTING_VECTOR_PROVIDERS, vectorProviders);
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
  private record VectorProviderResponse(
      String url,
      List<String> supportedAuthentication,
      List<PropertyBasedEmbeddingServiceConfig.VectorProviderConfig.ParameterConfig> parameters,
      List<ModelConfigResponse> models) {
    private static VectorProviderResponse provider(
        PropertyBasedEmbeddingServiceConfig.VectorProviderConfig vectorProviderConfig) {
      ArrayList<ModelConfigResponse> modelsRemoveProperties = new ArrayList<>();
      for (PropertyBasedEmbeddingServiceConfig.VectorProviderConfig.ModelConfig model :
          vectorProviderConfig.models()) {
        ModelConfigResponse returnModel = new ModelConfigResponse(model.name(), model.parameters());
        modelsRemoveProperties.add(returnModel);
      }
      return new VectorProviderResponse(
          vectorProviderConfig.url(),
          vectorProviderConfig.supportedAuthentication(),
          vectorProviderConfig.parameters(),
          modelsRemoveProperties);
    }
  }

  /**
   * Configuration details for a model offered by a vector provider, tailored for external clients.
   * Includes the model name and parameters for customization, excluding internal properties (retry,
   * timeout etc.).
   *
   * @param name Identifier name of the model.
   * @param parameters Parameters for customizing the model.
   */
  private record ModelConfigResponse(
      String name,
      List<PropertyBasedEmbeddingServiceConfig.VectorProviderConfig.ParameterConfig> parameters) {}
}
