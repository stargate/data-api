package io.stargate.sgv2.jsonapi.service.operation.rerank;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfigImpl;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record FindRerankProvidersOperation(RerankProvidersConfig config) implements Operation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, FindRerankProvidersOperation.RerankProviderResponse> rerankProviders =
                  config.providers().entrySet().stream()
                      .filter(entry -> entry.getValue().enabled())
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              entry ->
                                  FindRerankProvidersOperation.RerankProviderResponse.provider(
                                      entry.getValue())));
              return new Result(rerankProviders);
            });
  }

  // simple result wrapper
  private record Result(Map<String, RerankProviderResponse> rerankProviders)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {

      return CommandResult.statusOnlyBuilder(false, false)
          .addStatus(CommandStatus.EXISTING_RERANK_PROVIDERS, rerankProviders)
          .build();
    }
  }

  private record RerankProviderResponse(
      boolean isDefault,
      String displayName,
      Map<
              RerankProvidersConfig.RerankProviderConfig.AuthenticationType,
              RerankProvidersConfig.RerankProviderConfig.AuthenticationConfig>
          supportedAuthentication,
      List<RerankProvidersConfig.RerankProviderConfig.ModelConfig> models) {
    private static RerankProviderResponse provider(
        RerankProvidersConfig.RerankProviderConfig rerankProviderConfig) {
      return new RerankProviderResponse(
          rerankProviderConfig.isDefault(),
          rerankProviderConfig.displayName(),
          rerankProviderConfig.supportedAuthentications(),
          excludeProperties(rerankProviderConfig.models()));
    }

    // exclude internal model properties from findRerankProviders command
    private static List<RerankProvidersConfig.RerankProviderConfig.ModelConfig> excludeProperties(
        List<RerankProvidersConfig.RerankProviderConfig.ModelConfig> models) {
      return models.stream()
          .map(
              model ->
                  new RerankProvidersConfigImpl.RerankProviderConfigImpl.ModelConfigImpl(
                      model.name(), model.isDefault(), model.url(), null))
          .collect(Collectors.toList());
    }
  }
}
