package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.provider.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.provider.reranking.configuration.RerankingProvidersConfigImpl;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record FindRerankingProvidersOperation(RerankingProvidersConfig config)
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
                              entry -> RerankingProviderResponse.provider(entry.getValue())));
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
    private static RerankingProviderResponse provider(
        RerankingProvidersConfig.RerankingProviderConfig rerankingProviderConfig) {
      return new RerankingProviderResponse(
          rerankingProviderConfig.isDefault(),
          rerankingProviderConfig.displayName(),
          rerankingProviderConfig.supportedAuthentications(),
          excludeProperties(rerankingProviderConfig.models()));
    }

    // exclude internal model properties from findRerankingProviders command
    private static List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig>
        excludeProperties(
            List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models) {
      return models.stream()
          .map(
              model ->
                  new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
                      model.name(), model.isDefault(), model.url(), null))
          .collect(Collectors.toList());
    }
  }
}
