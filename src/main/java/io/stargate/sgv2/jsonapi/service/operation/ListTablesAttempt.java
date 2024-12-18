package io.stargate.sgv2.jsonapi.service.operation;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.List;

/** Attempt to list tables in a keyspace. */
public class ListTablesAttempt extends MetadataAttempt<KeyspaceSchemaObject> {

  protected ListTablesAttempt(int position, KeyspaceSchemaObject schemaObject) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);
    setStatus(OperationStatus.READY);
  }

  /**
   * Get table names from the keyspace metadata.
   *
   * @return List of table names.
   */
  @Override
  protected List<String> getNames() {

    // TODO: BETTER CONTROL on KEYSPACE OPTIONAL
    return keyspaceMetadata
        .get()
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
    // TODO: BETTER CONTROL on KEYSPACE OPTIONAL
    return keyspaceMetadata
        .get()
        // get all tables
        .getTables()
        .values()
        .stream()
        .filter(TABLE_MATCHER)
        .map(tableMetadata -> TableSchemaObject.from(tableMetadata, OBJECT_MAPPER))
        .map(tableSchemaObject -> tableSchemaObject.apiTableDef().toTableDesc())
        .toList();
  }
}
