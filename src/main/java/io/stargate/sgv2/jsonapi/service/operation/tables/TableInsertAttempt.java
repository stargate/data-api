package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowId;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

public class TableInsertAttempt implements InsertAttempt {

  private final int position;
  private final RowId rowId;
  private final WriteableTableRow row;
  private Throwable failure;

  private TableInsertAttempt(int position, RowId rowId, WriteableTableRow row) {
    this.position = position;
    this.rowId = rowId;
    this.row = row;
  }

  public static List<TableInsertAttempt> create(RowShredder shredder, JsonNode document) {
    return create(shredder, List.of(document));
  }

  public static List<TableInsertAttempt> create(RowShredder shredder, List<JsonNode> documents) {
    Objects.requireNonNull(shredder, "shredder cannot be null");
    Objects.requireNonNull(documents, "documents cannot be null");

    // TODO, just use a for loop, instead of IntStream
    return IntStream.range(0, documents.size())
        .mapToObj(
            i -> {
              WriteableTableRow row;
              try {
                row = shredder.shred(documents.get(i));
              } catch (Exception e) {
                // TODO: need a shredding base excpetion to catch
                // TODO: we need to get the row id, so we can return it in the response
                return (TableInsertAttempt)
                    new TableInsertAttempt(i, null, null).maybeAddFailure(e);
              }
              return new TableInsertAttempt(i, row.id(), row);
            })
        .toList();
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