package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowId;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TableInsertAttempt implements InsertAttempt {

  private final TableSchemaObject tableSchemaObject;
  private final int position;
  private final RowId rowId;
  private final WriteableTableRow row;
  private Throwable failure;

  private TableInsertAttempt(
      TableSchemaObject tableSchemaObject, int position, RowId rowId, WriteableTableRow row) {
    this.tableSchemaObject =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    this.position = position;
    this.rowId = rowId;
    this.row = row;
  }

  public static List<TableInsertAttempt> create(
      RowShredder shredder, TableSchemaObject table, JsonNode document) {
    return create(shredder, table, List.of(document));
  }

  public static List<TableInsertAttempt> create(
      RowShredder shredder, TableSchemaObject tableSchemaObject, List<JsonNode> documents) {
    Objects.requireNonNull(shredder, "shredder cannot be null");
    Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    Objects.requireNonNull(documents, "documents cannot be null");

    final List<TableInsertAttempt> attempts = new ArrayList<>(documents.size());
    for (int i = 0; i < documents.size(); i++) {
      try {
        WriteableTableRow row = shredder.shred(tableSchemaObject, documents.get(i));

        // validating may throw
        SchemaValidatable.maybeValidate(tableSchemaObject, row);
        attempts.add(new TableInsertAttempt(tableSchemaObject, i, row.id(), row));
      } catch (Exception e) {
        // TODO: need a shredding base exception to catch
        // TODO: we need to get the row id, so we can return it in the response
        attempts.add(
            (TableInsertAttempt)
                new TableInsertAttempt(tableSchemaObject, i, null, null).maybeAddFailure(e));
      }
    }
    return attempts;
  }

  public TableInsertValuesCQLClause getInsertValuesCQLClause() {
    return new TableInsertValuesCQLClause(tableSchemaObject, row);
  }

  public Optional<WriteableTableRow> row() {
    return Optional.ofNullable(row);
  }

  @Override
  public int position() {
    return position;
  }

  @Override
  public Optional<DocRowIdentifer> docRowID() {
    return Optional.ofNullable(rowId);
  }

  @Override
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  @Override
  public InsertAttempt maybeAddFailure(Throwable failure) {
    if (this.failure == null) {
      this.failure = failure;
    }
    return this;
  }
}
