package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** For now, a marker class / interface for operations that modify data in a table. */
abstract class TableMutationOperation extends TableOperation {

  protected TableMutationOperation(CommandContext<TableSchemaObject> commandContext) {
    super(commandContext);
  }
}
