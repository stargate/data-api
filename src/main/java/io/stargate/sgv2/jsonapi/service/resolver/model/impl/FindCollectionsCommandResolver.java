package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.FindCollectionsOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for the {@link FindCollectionsCommand}. */
@ApplicationScoped
public class FindCollectionsCommandResolver implements CommandResolver<FindCollectionsCommand> {
  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;

  @Inject
  public FindCollectionsCommandResolver(
      ObjectMapper objectMapper, CQLSessionCache cqlSessionCache) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
  }

  /** {@inheritDoc} */
  @Override
  public Class<FindCollectionsCommand> getCommandClass() {
    return FindCollectionsCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, FindCollectionsCommand command) {

    boolean explain = command.options() != null ? command.options().explain() : false;
    return new FindCollectionsOperation(explain, objectMapper, cqlSessionCache, ctx);
  }
}
