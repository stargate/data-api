package io.stargate.sgv2.jsonapi.service.operation.rerank;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.rerank.configuration.RerankProvidersConfig;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public record FindRerankProvidersOperation(RerankProvidersConfig config) implements Operation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    return Uni.createFrom()
        .item(
            () -> {
              Map<String, FindRerankProvidersOperation.RerankProviderResponse> embeddingProviders =
                  config.providers().entrySet().stream()
                      .filter(entry -> entry.getValue().enabled())
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              entry ->
                                  FindRerankProvidersOperation.RerankProviderResponse.provider(
                                      entry.getValue())));
              return new Result(embeddingProviders);
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
      String displayName,
      Map<
              RerankProvidersConfig.RerankProviderConfig.AuthenticationType,
              RerankProvidersConfig.RerankProviderConfig.AuthenticationConfig>
          supportedAuthentication,
      List<RerankProvidersConfig.RerankProviderConfig.ModelConfig> models) {
    private static RerankProviderResponse provider(
        RerankProvidersConfig.RerankProviderConfig rerankProviderConfig) {

      return new RerankProviderResponse(
          rerankProviderConfig.displayName(),
          rerankProviderConfig.supportedAuthentications(),
          rerankProviderConfig.models());
    }
  }
}
