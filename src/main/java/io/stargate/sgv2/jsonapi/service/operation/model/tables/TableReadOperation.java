package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** For now, a marker class / interface for operations that read data in a table. */
abstract class TableReadOperation extends TableOperation {

  protected final LogicalExpression logicalExpression;

  public TableReadOperation(
      CommandContext<TableSchemaObject> commandContext, LogicalExpression logicalExpression) {
    super(commandContext);
    Preconditions.checkNotNull(logicalExpression, "logicalExpression cannot be null");
    this.logicalExpression = logicalExpression;
  }
}
