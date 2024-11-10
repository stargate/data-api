package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDefContainer;
import java.util.List;
import java.util.Optional;

/** Attempt to list indexes for a table. */
public class ListIndexesAttempt extends MetadataAttempt<TableSchemaObject> {

  protected ListIndexesAttempt(int position, TableSchemaObject schemaObject) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);
    setStatus(OperationStatus.READY);
  }

  private Optional<ApiIndexDefContainer> indexesForTable() {
    // TODO: better option checking
    var tableMetadata =
        keyspaceMetadata.get().getTable(schemaObject.tableMetadata().getName()).get();

    // aaron - this should not happen ?
    if (!TABLE_MATCHER.test(tableMetadata)) {
      return Optional.empty();
    }
    return Optional.of(
        TableSchemaObject.from(tableMetadata, OBJECT_MAPPER).apiTableDef().indexes());
  }

  /**
   * Get indexes names from the table metadata.
   *
   * @return List of index names.
   */
  @Override
  protected List<String> getNames() {

    return indexesForTable()
        .map(
            indexes ->
                indexes.allIndexes().stream()
                    .map(index -> cqlIdentifierToJsonKey(index.indexName()))
                    .toList())
        .orElse(List.of());
  }

  /**
   * Get indexes schema for the tables.
   *
   * @return List of indexes schema as Object.
   */
  @Override
  protected Object getSchema() {
    return indexesForTable()
        .map(indexes -> indexes.allIndexes().stream().map(ApiIndexDef::indexDesc).toList())
        .orElse(List.of());
  }
}
