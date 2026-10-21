package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.SearchAggregateCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SearchAggregateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.SearchAggregateTableOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link SearchAggregateCommand} for collection and table targets. */
@ApplicationScoped
public class SearchAggregateCommandResolver implements CommandResolver<SearchAggregateCommand> {

  private final ObjectMapper objectMapper;

  @Inject
  public SearchAggregateCommandResolver(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<SearchAggregateCommand> getCommandClass() {
    return SearchAggregateCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> commandContext, SearchAggregateCommand command) {
    return SearchAggregateCollectionOperation.of(
        commandContext, command.aggregations(), command.query(), objectMapper);
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, SearchAggregateCommand command) {
    return SearchAggregateTableOperation.of(
        commandContext, command.aggregations(), command.query(), objectMapper);
  }
}
