package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingServiceConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.HashMap;
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
              // TODO: get all available vector providers
              Map<String, PropertyBasedEmbeddingServiceConfig.VectorProviderConfig>
                  vectorProviders = new HashMap<>();
              if (propertyBasedEmbeddingServiceConfig.google().enabled()) {
                vectorProviders.put("google", propertyBasedEmbeddingServiceConfig.google());
              }
              return new Result(vectorProviders);
            });
  }

  // simple result wrapper
  private record Result(
      Map<String, PropertyBasedEmbeddingServiceConfig.VectorProviderConfig> vectorProviders)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses =
          Map.of(CommandStatus.EXISTING_VECTOR_PROVIDERS, vectorProviders);
      return new CommandResult(statuses);
    }
  }
}
