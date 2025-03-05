package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import java.util.Objects;

/**
 * Base for any operations that works with CQL Tables with rows, rather than Collections of
 * Documents * 26 sept 2024 - aaron - this will be removed when we migrate the delete and update
 * table ops to use OperationAttempt
 */
abstract class TableOperation implements Operation<TableSchemaObject> {

  protected final CommandContext<TableSchemaObject> commandContext;

  protected TableOperation(CommandContext<TableSchemaObject> commandContext) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");
  }
}
