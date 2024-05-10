package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Operation that list all available and enabled vector providers into the {@link
 * CommandStatus#EXISTING_VECTOR_PROVIDERS} command status.
 */
public record FindEmbeddingProvidersOperation(EmbeddingProvidersConfig config)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, EmbeddingProviderResponse> embeddingProviders =
                  config.providers().entrySet().stream()
                      .filter(entry -> entry.getValue().enabled())
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              entry -> EmbeddingProviderResponse.provider(entry.getValue())));
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
      String displayName,
      String url,
      Map<
              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType,
              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig>
          supportedAuthentication,
      List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> parameters,
      List<ModelConfigResponse> models) {
    private static EmbeddingProviderResponse provider(
        EmbeddingProvidersConfig.EmbeddingProviderConfig embeddingProviderConfig) {
      ArrayList<ModelConfigResponse> modelsRemoveProperties = new ArrayList<>();
      for (EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model :
          embeddingProviderConfig.models()) {
        ModelConfigResponse returnModel =
            ModelConfigResponse.returnModelConfigResponse(
                model.name(), model.vectorDimension(), model.parameters());
        modelsRemoveProperties.add(returnModel);
      }
      return new EmbeddingProviderResponse(
          embeddingProviderConfig.displayName(),
          embeddingProviderConfig.url(),
          embeddingProviderConfig.supportedAuthentications(),
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
      String name, Optional<Integer> vectorDimension, List<ParameterConfigResponse> parameters) {
    private static ModelConfigResponse returnModelConfigResponse(
        String name,
        Optional<Integer> vectorDimension,
        List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> parameters) {
      // reconstruct each parameter for lowercase parameter type
      ArrayList<ParameterConfigResponse> parametersResponse = new ArrayList<>();
      for (EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig parameter :
          parameters) {
        ParameterConfigResponse returnParameter =
            ParameterConfigResponse.returnParameterConfigResponse(
                parameter.name(),
                parameter.type().toString(),
                parameter.required(),
                parameter.defaultValue(),
                parameter.validation(),
                parameter.help());
        parametersResponse.add(returnParameter);
      }

      return new ModelConfigResponse(name, vectorDimension, parametersResponse);
    }
  }

  /**
   * Represents the configuration details for a parameter of a model. This is used to reconstruct
   * the {@code PropertyBasedEmbeddingProviderConfig.EmbeddingProviderConfig.ParameterConfig} body
   * by not directly using the enum class (uppercase) but instead using the value (lowercase) in the
   * enum class. It transforms the parameter type and validation fields to lowercase.
   *
   * @param name
   * @param type
   * @param required
   * @param defaultValue
   * @param validation
   * @param help
   */
  private record ParameterConfigResponse(
      String name,
      String type,
      boolean required,
      Optional<String> defaultValue,
      Map<String, List<Integer>> validation,
      Optional<String> help) {
    private static ParameterConfigResponse returnParameterConfigResponse(
        String name,
        String type,
        boolean required,
        Optional<String> defaultValue,
        Map<EmbeddingProvidersConfig.EmbeddingProviderConfig.ValidationType, List<Integer>>
            validation,
        Optional<String> help) {
      Map<String, List<Integer>> validationMap = new HashMap<>();
      for (Map.Entry<EmbeddingProvidersConfig.EmbeddingProviderConfig.ValidationType, List<Integer>>
          entry : validation.entrySet()) {
        validationMap.put(entry.getKey().toString(), entry.getValue());
      }
      return new ParameterConfigResponse(name, type, required, defaultValue, validationMap, help);
    }
  }
}
