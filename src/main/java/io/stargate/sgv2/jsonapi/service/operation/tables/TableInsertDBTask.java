package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertDBTask;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowId;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.Optional;

/**
 * An attempt to insert a row into an API Table, overrides the {@link InsertDBTask} to provide the
 * row id and schema.
 */
public class TableInsertDBTask extends InsertDBTask<TableSchemaObject> {

  private final RowId rowId;
  private final WriteableTableRow row;

  TableInsertDBTask(
      int position,
      TableSchemaObject tableSchemaObject,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory,
      RowId rowId,
      WriteableTableRow row) {
    super(
        position,
        tableSchemaObject,
        exceptionHandlerFactory,
        row == null ? null : new TableInsertValuesCQLClause(tableSchemaObject, row));

    this.rowId = rowId;
    this.row = row;
    setStatus(TaskStatus.READY);
  }

  public static TableInsertDBTaskBuilder builder(CommandContext<TableSchemaObject> commandContext) {
    return new TableInsertDBTaskBuilder(commandContext);
  }

  @Override
  public Optional<DocRowIdentifer> docRowID() {
    return Optional.ofNullable(rowId);
  }

  /** Override to describe the schema of the primary keys in the row we inserted */
  @Override
  public Optional<ColumnsDescContainer> schemaDescription() {

    /// we could be in an error state, and not have inserted anything , if we got a shredding error
    // then we do not have the row to describe
    if (row == null) {
      return Optional.empty();
    }
    var apiColumns = schemaObject.apiTableDef().primaryKeys();
    var unsupported = apiColumns.filterBySupport(x -> !x.insert());
    if (!unsupported.isEmpty()) {
      throw new IllegalStateException("Unsupported columns primary key: %s" + unsupported);
    }

    return Optional.of(apiColumns.toColumnsDesc());
  }
}
