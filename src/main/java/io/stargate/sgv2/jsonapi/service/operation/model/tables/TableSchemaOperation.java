package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** For now, a marker class / interface for operations that modify schema */
abstract class TableSchemaOperation extends TableOperation {

  protected TableSchemaOperation(CommandContext<TableSchemaObject> commandContext) {
    super(commandContext);
  }
}
