package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record FindRerankingProvidersOperation(
    FindRerankingProvidersCommand command, RerankingProvidersConfig config)
    implements Operation<DatabaseSchemaObject> {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, RerankingProviderResponse> rerankingProviders =
                  config.providers().entrySet().stream()
                      .filter(entry -> entry.getValue().enabled())
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              entry ->
                                  RerankingProviderResponse.toResponse(
                                      entry.getValue(), getSupportStatusPredicate())));
              return new Result(rerankingProviders);
            });
  }

  // simple result wrapper
  private record Result(Map<String, RerankingProviderResponse> rerankingProviders)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {

      return CommandResult.statusOnlyBuilder(false, false, RequestTracing.NO_OP)
          .addStatus(CommandStatus.EXISTING_RERANKING_PROVIDERS, rerankingProviders)
          .build();
    }
  }

  private record RerankingProviderResponse(
      boolean isDefault,
      String displayName,
      Map<
              RerankingProvidersConfig.RerankingProviderConfig.AuthenticationType,
              RerankingProvidersConfig.RerankingProviderConfig.AuthenticationConfig>
          supportedAuthentication,
      List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models) {

    /**
     * Constructs an {@link RerankingProviderResponse} from the original provider config.
     *
     * @param sourceRerankingProviderConfig, the original provider config with all properties.
     * @param statusPredicate predicate to filter models based on their support status.
     */
    private static RerankingProviderResponse toResponse(
        RerankingProvidersConfig.RerankingProviderConfig sourceRerankingProviderConfig,
        Predicate<ApiModelSupport.SupportStatus> statusPredicate) {

      // if the providerConfig.models is null or empty, return an EmbeddingProviderResponse with
      // empty models.
      if (sourceRerankingProviderConfig.models() == null
          || sourceRerankingProviderConfig.models().isEmpty()) {
        return new RerankingProviderResponse(
            sourceRerankingProviderConfig.isDefault(),
            sourceRerankingProviderConfig.displayName(),
            sourceRerankingProviderConfig.supportedAuthentications(),
            Collections.emptyList());
      }

      // include models that with apiModelSupport status that user asked for.
      // also exclude internal model properties.
      var filteredModels = filteredModels(sourceRerankingProviderConfig.models(), statusPredicate);

      return new RerankingProviderResponse(
          sourceRerankingProviderConfig.isDefault(),
          sourceRerankingProviderConfig.displayName(),
          sourceRerankingProviderConfig.supportedAuthentications(),
          filteredModels);
    }

    /**
     * Returns models matched by given model supportStatus Predicate, and exclude internal model
     * properties from command response.
     */
    private static List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig>
        filteredModels(
            List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models,
            Predicate<ApiModelSupport.SupportStatus> statusPredicate) {
      return models.stream()
          .filter(modelConfig -> statusPredicate.test(modelConfig.apiModelSupport().status()))
          .map(
              model ->
                  new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
                      model.name(), model.apiModelSupport(), model.isDefault(), model.url(), null))
          .sorted(
              Comparator.comparing(
                  RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::name))
          .collect(Collectors.toList());
    }
  }

  /**
   * With {@link FindRerankingProvidersCommand.Options#filterModelStatus()}, there are the rules to
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
