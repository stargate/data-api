package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindEmbeddingProvidersCommand;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindEmbeddingProvidersOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindEmbeddingProvidersCommand}. */
@ApplicationScoped
public class FindEmbeddingProvidersCommandResolver
    implements CommandResolver<FindEmbeddingProvidersCommand> {

  @Inject EmbeddingProvidersConfig embeddingProvidersConfig;

  public FindEmbeddingProvidersCommandResolver() {}

  @Override
  public Class<FindEmbeddingProvidersCommand> getCommandClass() {
    return FindEmbeddingProvidersCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, FindEmbeddingProvidersCommand command) {
    return new FindEmbeddingProvidersOperation(embeddingProvidersConfig);
  }
}
