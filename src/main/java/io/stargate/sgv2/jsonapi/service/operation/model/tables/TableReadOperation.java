package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** For now, a marker class / interface for operations that read data in a table. */
abstract class TableReadOperation extends TableOperation {

  protected final CommandContext<TableSchemaObject> commandContext;
  protected final LogicalExpression logicalExpression;

  public TableReadOperation(
      CommandContext<TableSchemaObject> commandContext, LogicalExpression logicalExpression) {

    Preconditions.checkNotNull(commandContext, "commandContext cannot be null");
    Preconditions.checkNotNull(logicalExpression, "logicalExpression cannot be null");
    this.commandContext = commandContext;
    this.logicalExpression = logicalExpression;
  }
}
