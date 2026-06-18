package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionsCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for the {@link FindCollectionsCommand}. */
@ApplicationScoped
public class FindCollectionsCommandResolver implements CommandResolver<FindCollectionsCommand> {
  private final ObjectMapper objectMapper;

  @Inject
  public FindCollectionsCommandResolver(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
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
    return new FindCollectionsCollectionOperation(
        explain, objectMapper, ctx.cqlSessionCache(), ctx);
  }
}
