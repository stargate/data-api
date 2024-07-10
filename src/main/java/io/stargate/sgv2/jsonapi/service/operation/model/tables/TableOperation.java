package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;

/**
 * Base for any operations that works with CQL Tables with rows, rather than Collections of
 * Documents
 */
abstract class TableOperation implements Operation {

  protected final CommandContext<TableSchemaObject> commandContext;

  protected TableOperation(CommandContext<TableSchemaObject> commandContext) {
    Preconditions.checkNotNull(commandContext, "commandContext cannot be null");
    this.commandContext = commandContext;
  }
}
