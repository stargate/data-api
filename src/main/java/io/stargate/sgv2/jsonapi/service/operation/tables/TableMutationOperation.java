package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;

/**
 * For now, a marker class / interface for operations that modify data in a table. 26 sept 2024 -
 * aaron - this will be removed when we migrate the delete and update table ops to use
 * OperationAttempt
 */
abstract class TableMutationOperation extends TableOperation {

  protected TableMutationOperation(CommandContext<TableSchemaObject> commandContext) {
    super(commandContext);
  }
}
