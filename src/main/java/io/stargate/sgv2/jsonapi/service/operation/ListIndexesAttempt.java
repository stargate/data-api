package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDefContainer;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Attempt to list indexes for a table. */
public class ListIndexesAttempt extends MetadataAttempt<TableSchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ListIndexesAttempt.class);

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
    var indexesContainer =
        TableSchemaObject.from(tableMetadata, OBJECT_MAPPER)
            .apiTableDef()
            .indexesIncludingUnsupported();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "indexesForTable() - table: {} indexesContainer: {}",
          schemaObject.tableMetadata().getName(),
          indexesContainer);
    }
    return Optional.of(indexesContainer);
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
