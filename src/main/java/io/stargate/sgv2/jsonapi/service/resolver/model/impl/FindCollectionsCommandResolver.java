package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindCollectionsOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for the {@link FindCollectionsCommand}. */
@ApplicationScoped
public class FindCollectionsCommandResolver implements CommandResolver<FindCollectionsCommand> {

  private final SchemaManager schemaManager;
  private final ObjectMapper objectMapper;

  @Inject
  public FindCollectionsCommandResolver(SchemaManager schemaManager, ObjectMapper objectMapper) {
    this.schemaManager = schemaManager;
    this.objectMapper = objectMapper;
  }

  /** {@inheritDoc} */
  @Override
  public Class<FindCollectionsCommand> getCommandClass() {
    return FindCollectionsCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveCommand(CommandContext ctx, FindCollectionsCommand command) {
    boolean explain = command.options() != null ? command.options().explain() : false;
    return new FindCollectionsOperation(explain, objectMapper, schemaManager, ctx);
  }
}
