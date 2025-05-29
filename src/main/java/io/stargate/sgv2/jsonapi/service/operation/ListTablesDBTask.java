package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.List;

/** Attempt to list tables in a keyspace. */
public class ListTablesDBTask extends MetadataDBTask<KeyspaceSchemaObject> {

  public ListTablesDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory) {
    super(position, schemaObject, TaskRetryPolicy.NO_RETRY, exceptionHandlerFactory);
    setStatus(TaskStatus.READY);
  }

  public static TaskBuilder.BasicTaskBuilder<ListTablesDBTask, KeyspaceSchemaObject> builder(
      KeyspaceSchemaObject schemaObject) {
    return new TaskBuilder.BasicTaskBuilder<>(schemaObject, ListTablesDBTask::new);
  }

  /**
   * Get table names from the keyspace metadata.
   *
   * @return List of table names.
   */
  @Override
  protected List<String> getNames() {

    if (keyspaceMetadata == null) {
      throw new IllegalStateException("keyspaceMetadata should not be null when generating result");
    }
    return keyspaceMetadata.getTables().values().stream()
        .filter(TABLE_MATCHER)
        .map(tableMetadata -> cqlIdentifierToJsonKey(tableMetadata.getName()))
        .toList();
  }

  /**
   * Get tables schema for all the tables schemas in the keyspace.
   *
   * @return List of table schema as Object.
   */
  @Override
  protected Object getSchema() {

    if (keyspaceMetadata == null) {
      throw new IllegalStateException("keyspaceMetadata should not be null when generating result");
    }
    return keyspaceMetadata.getTables().values().stream()
        .filter(TABLE_MATCHER)
        .map(
            tableMetadata ->
                TableSchemaObject.from(
                    lastResultSupplier().commandContext().requestContext().tenant(),
                    tableMetadata,
                    OBJECT_MAPPER))
        .map(tableSchemaObject -> tableSchemaObject.apiTableDef().toTableDesc())
        .toList();
  }
}
