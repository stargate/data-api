package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Operation that list all available and enabled vector providers into the {@link
 * CommandStatus#EXISTING_EMBEDDING_PROVIDERS} command status.
 */
public record FindEmbeddingProvidersOperation(
    FindEmbeddingProvidersCommand command, EmbeddingProvidersConfig config) implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, EmbeddingProviderResponse> embeddingProviders =
                  config.providers().entrySet().stream()
                      .filter(entry -> entry.getValue().enabled())
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              entry ->
                                  EmbeddingProviderResponse.toResponse(
                                      entry.getValue(), getSupportStatusPredicate())));
              return new Result(embeddingProviders);
            });
  }

  // simple result wrapper
  private record Result(Map<String, EmbeddingProviderResponse> embeddingProviders)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {

      return CommandResult.statusOnlyBuilder(false, false, RequestTracing.NO_OP)
          .addStatus(CommandStatus.EXISTING_EMBEDDING_PROVIDERS, embeddingProviders)
          .build();
    }
  }

  /**
   * A simplified representation of n embedding provider's configuration for API responses. Excludes
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
      Optional<String> url,
      Map<
              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType,
              EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig>
          supportedAuthentication,
      List<EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig> parameters,
      List<ModelConfigResponse> models) {

    /**
     * Constructs an {@link EmbeddingProviderResponse} from the original provider config. It will
     * exclude the internal properties (retry, timeout etc.).
     *
     * @param sourceEmbeddingProviderConfig, the original provider config with all properties.
     * @param statusPredicate predicate to filter models based on their support status.
     */
    private static EmbeddingProviderResponse toResponse(
        EmbeddingProvidersConfig.EmbeddingProviderConfig sourceEmbeddingProviderConfig,
        Predicate<ApiModelSupport.SupportStatus> statusPredicate) {

      // if the providerConfig.models is null or empty, return an EmbeddingProviderResponse with
      // empty models.
      if (sourceEmbeddingProviderConfig.models() == null
          || sourceEmbeddingProviderConfig.models().isEmpty()) {
        return new EmbeddingProviderResponse(
            sourceEmbeddingProviderConfig.displayName(),
            sourceEmbeddingProviderConfig.url(),
            sourceEmbeddingProviderConfig.supportedAuthentications(),
            sourceEmbeddingProviderConfig.parameters(),
            Collections.emptyList());
      }

      // include models that with apiModelSupport status that user asked for
      var modelsFilteredWithStatus =
          sourceEmbeddingProviderConfig.models().stream()
              .filter(modelConfig -> statusPredicate.test(modelConfig.apiModelSupport().status()))
              .toList();

      // convert each modelConfig to ModelConfigResponse with internal properties excluded
      var modelConfigResponses =
          modelsFilteredWithStatus.stream()
              .map(ModelConfigResponse::toResponse)
              .sorted(Comparator.comparing(ModelConfigResponse::name))
              .toList();

      return new EmbeddingProviderResponse(
          sourceEmbeddingProviderConfig.displayName(),
          sourceEmbeddingProviderConfig.url(),
          sourceEmbeddingProviderConfig.supportedAuthentications(),
          sourceEmbeddingProviderConfig.parameters(),
          modelConfigResponses);
    }
  }

  /**
   * Model configuration with internal properties excluded. Only includes the model name and
   * parameters for customization, excluding internal properties (retry, timeout etc.).
   *
   * @param name Identifier name of the model.
   * @param apiModelSupport Support status of the model.
   * @param vectorDimension vector dimension of the model.
   * @param parameters Parameters for customizing the model.
   */
  private record ModelConfigResponse(
      String name,
      ApiModelSupport apiModelSupport,
      Optional<Integer> vectorDimension,
      List<ParameterConfigResponse> parameters) {

    private static ModelConfigResponse toResponse(
        EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig sourceModelConfig) {

      // if the sourceModelConfig.parameters is null or empty, return a ModelConfigResponse with
      // empty parameters.
      if (sourceModelConfig.parameters() == null || sourceModelConfig.parameters().isEmpty()) {
        return new ModelConfigResponse(
            sourceModelConfig.name(),
            sourceModelConfig.apiModelSupport(),
            sourceModelConfig.vectorDimension(),
            Collections.emptyList());
      }

      // reconstruct each parameter for lowercase parameter type
      List<ParameterConfigResponse> parametersResponse =
          sourceModelConfig.parameters().stream()
              .map(ParameterConfigResponse::toResponse)
              .collect(Collectors.toList());

      return new ModelConfigResponse(
          sourceModelConfig.name(),
          sourceModelConfig.apiModelSupport(),
          sourceModelConfig.vectorDimension(),
          parametersResponse);
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

    private static ParameterConfigResponse toResponse(
        EmbeddingProvidersConfig.EmbeddingProviderConfig.ParameterConfig sourceParameterConfig) {
      Map<String, List<Integer>> validationMap =
          sourceParameterConfig.validation().entrySet().stream()
              .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));

      return new ParameterConfigResponse(
          sourceParameterConfig.name(),
          sourceParameterConfig.type().name(),
          sourceParameterConfig.required(),
          sourceParameterConfig.defaultValue(),
          validationMap,
          sourceParameterConfig.help());
    }
  }

  /**
   * With {@link FindEmbeddingProvidersCommand.Options#filterModelStatus()}, there are the rules to
   * filter the models:
   *
   * <ul>
   *   <li>If not provided, only SUPPORTED models will be returned.
   *   <li>If provided with "" empty string or null, all SUPPORTED, DEPRECATED, END_OF_LIFE model
   *       will be returned.
   *   <li>If provided with specified SUPPORTED or DEPRECATED or END_OF_LIFE, only models matched
   *       the status will be returned.
   * </ul>
   */
  private Predicate<ApiModelSupport.SupportStatus> getSupportStatusPredicate() {
    if (command.options() == null) {
      return status -> status == ApiModelSupport.SupportStatus.SUPPORTED;
    }

    if (command.options().filterModelStatus() == null
        || command.options().filterModelStatus().isBlank()) {
      return status -> true; // accept all
    }

    return status -> status.name().equalsIgnoreCase(command.options().filterModelStatus());
  }
}
