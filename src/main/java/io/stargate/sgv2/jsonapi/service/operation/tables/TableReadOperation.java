package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.Objects;

/** For now, a marker class / interface for operations that read data in a table. */
abstract class TableReadOperation extends TableOperation {

  public TableReadOperation(
      CommandContext<TableSchemaObject> commandContext) {
    super(commandContext);
  }
}
