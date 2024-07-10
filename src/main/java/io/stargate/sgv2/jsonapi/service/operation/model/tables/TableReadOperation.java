package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import java.util.Objects;

/** For now, a marker class / interface for operations that read data in a table. */
abstract class TableReadOperation extends TableOperation {

  protected final CommandContext<CollectionSchemaObject> commandContext;
  protected final LogicalExpression logicalExpression;

  public TableReadOperation(
      CommandContext<CollectionSchemaObject> commandContext, LogicalExpression logicalExpression) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");
    this.logicalExpression =
        Objects.requireNonNull(logicalExpression, "logicalExpression cannot be null");
  }
}
