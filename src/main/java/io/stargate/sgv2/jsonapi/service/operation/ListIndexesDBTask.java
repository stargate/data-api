package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDefContainer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Attempt to list indexes for a table. */
public class ListIndexesDBTask extends MetadataDBTask<TableSchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ListIndexesDBTask.class);

  public ListIndexesDBTask(
      int position,
      TableSchemaObject schemaObject,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);
    setStatus(TaskStatus.READY);
  }

  public static TaskBuilder.BasicTaskBuilder<ListIndexesDBTask, TableSchemaObject> builder(
      TableSchemaObject schemaObject) {
    return new TaskBuilder.BasicTaskBuilder<>(schemaObject, ListIndexesDBTask::new);
  }

  private Optional<ApiIndexDefContainer> indexesForTable() {

    // aaron - see the MetadataDBTask, need better control on when this is set
    Objects.requireNonNull(
        keyspaceMetadata, "keyspaceMetadata must be set before calling getNames");

    var tableMetadata = keyspaceMetadata.getTable(schemaObject.tableMetadata().getName()).get();

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
        .map(
            indexes ->
                indexes.allIndexes().stream()
                    .map(
                        apiIndexDef ->
                            apiIndexDef.getSchemaDescription(SchemaDescSource.DDL_SCHEMA_OBJECT))
                    .toList())
        .orElse(List.of());
  }
}
