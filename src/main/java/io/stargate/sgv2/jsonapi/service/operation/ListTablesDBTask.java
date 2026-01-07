package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.List;
import java.util.Objects;

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

    // aaron - see the MetadataDBTask, need better control on when this is set
    Objects.requireNonNull(
        keyspaceMetadata, "keyspaceMetadata must be set before calling getNames");

    return keyspaceMetadata
        // get all tables
        .getTables()
        .values()
        .stream()
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

    // aaron - see the MetadataDBTask, need better control on when this is set
    Objects.requireNonNull(
        keyspaceMetadata, "keyspaceMetadata must be set before calling getNames");

    return keyspaceMetadata
        // get all tables
        .getTables()
        .values()
        .stream()
        .filter(TABLE_MATCHER)
        .map(
            tableMetadata ->
                TableSchemaObject.from(
                    schemaObject.identifier().tenant(), tableMetadata, OBJECT_MAPPER))
        .map(
            tableSchemaObject ->
                tableSchemaObject
                    .apiTableDef()
                    .getSchemaDescription(SchemaDescSource.DDL_SCHEMA_OBJECT))
        .toList();
  }
}
