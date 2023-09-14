package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateEmbeddingServiceCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingServiceConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateEmbeddingServiceOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Optional;

@ApplicationScoped
public class CreateEmbeddingServiceCommandResolver
    implements CommandResolver<CreateEmbeddingServiceCommand> {

  private EmbeddingServiceConfigStore embeddingServiceConfigStore;
  private StargateRequestInfo stargateRequestInfo;

  @Inject
  public CreateEmbeddingServiceCommandResolver(
      Instance<EmbeddingServiceConfigStore> embeddingServiceConfigStore,
      StargateRequestInfo stargateRequestInfo) {
    this.embeddingServiceConfigStore = embeddingServiceConfigStore.get();
    this.stargateRequestInfo = stargateRequestInfo;
  }

  @Override
  public Class<CreateEmbeddingServiceCommand> getCommandClass() {
    return CreateEmbeddingServiceCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateEmbeddingServiceCommand command) {

    if (embeddingServiceConfigStore instanceof PropertyBasedEmbeddingServiceConfig) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_COMMAND_EMBEDDING_SERVICE,
          ErrorCode.UNSUPPORTED_COMMAND_EMBEDDING_SERVICE.getMessage());
    }

    return new CreateEmbeddingServiceOperation(
        stargateRequestInfo == null ? Optional.empty() : stargateRequestInfo.getTenantId(),
        embeddingServiceConfigStore,
        command.name(),
        command.apiProvider(),
        command.baseUrl(),
        command.apiKey());
  }
}
