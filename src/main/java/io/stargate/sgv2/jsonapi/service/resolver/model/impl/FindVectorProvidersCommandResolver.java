package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindVectorProvidersCommand;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.PropertyBasedEmbeddingServiceConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindVectorProvidersOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindVectorProvidersCommand}. */
@ApplicationScoped
public class FindVectorProvidersCommandResolver
    implements CommandResolver<FindVectorProvidersCommand> {

  @Inject PropertyBasedEmbeddingServiceConfig propertyBasedEmbeddingServiceConfig;

  public FindVectorProvidersCommandResolver() {}

  @Override
  public Class<FindVectorProvidersCommand> getCommandClass() {
    return FindVectorProvidersCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, FindVectorProvidersCommand command) {
    return new FindVectorProvidersOperation(propertyBasedEmbeddingServiceConfig);
  }
}
