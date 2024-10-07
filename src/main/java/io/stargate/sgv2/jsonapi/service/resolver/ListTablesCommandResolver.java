package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListTablesCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.ListTablesOperation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for the {@link FindCollectionsCommand}. */
@ApplicationScoped
public class ListTablesCommandResolver implements CommandResolver<ListTablesCommand> {
  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;

  @Inject
  public ListTablesCommandResolver(ObjectMapper objectMapper, CQLSessionCache cqlSessionCache) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
  }

  /** {@inheritDoc} */
  @Override
  public Class<ListTablesCommand> getCommandClass() {
    return ListTablesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, ListTablesCommand command) {

    boolean explain = command.options() != null ? command.options().explain() : false;
    return new ListTablesOperation(explain, objectMapper, cqlSessionCache, ctx);
  }
}
