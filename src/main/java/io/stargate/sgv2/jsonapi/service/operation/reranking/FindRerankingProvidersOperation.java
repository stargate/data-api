package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRerankingProvidersCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                                  RerankingProviderResponse.provider(
                                      entry.getValue(), getSupportStatuses())));
              return new Result(rerankingProviders);
            });
  }

  // By default, if includeModelStatus is not provided in command option, only model in supported
  // status will be listed.
  private Set<
          RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport.SupportStatus>
      getSupportStatuses() {
    var includeModelStatus =
        Set.of(
            RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport.SupportStatus
                .SUPPORTED);
    if (command.options() != null && command.options().includeModelStatus() != null) {
      includeModelStatus = command.options().includeModelStatus();
    }
    return includeModelStatus;
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
    private static RerankingProviderResponse provider(
        RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig,
        Set<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport.SupportStatus>
            includeModelStatus) {

      return new RerankingProviderResponse(
          rerankingProviderConfig.isDefault(),
          rerankingProviderConfig.displayName(),
          rerankingProviderConfig.supportedAuthentications(),
          filteringModels(rerankingProviderConfig.models(), includeModelStatus));
    }

    // exclude internal model properties from findRerankingProviders command
    // exclude models that are not in the provided statuses
    private static List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig>
        filteringModels(
            List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models,
            Set<
                    RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.ModelSupport
                        .SupportStatus>
                includeModelStatus) {
      return models.stream()
          .filter(model -> includeModelStatus.contains(model.modelSupport().status()))
          .map(
              model ->
                  new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
                      model.name(), model.modelSupport(), model.isDefault(), model.url(), null))
          .sorted(
              Comparator.comparing(
                  RerankingProvidersConfig.RerankingProviderConfig.ModelConfig::name))
          .collect(Collectors.toList());
    }
  }
}
