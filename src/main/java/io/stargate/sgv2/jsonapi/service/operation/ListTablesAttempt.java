package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;

/** Attempt to list tables in a keyspace. */
public class ListTablesAttempt extends MetadataAttempt<KeyspaceSchemaObject> {

  private ListTablesAttempt(int position, KeyspaceSchemaObject schemaObject) {
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
    return getTables().stream()
        .map(
            schemaObject ->
                CqlIdentifierUtil.externalRepresentation(schemaObject.tableMetadata().getName()))
        .toList();
  }

  /**
   * Get tables schema for all the tables schemas in the keyspace.
   *
   * @return List of table schema as Object.
   */
  @Override
  protected Object getSchema() {
    return getTables().stream().map(schema -> getTableSchema(schema)).toList();
  }

  public static class ListTablesAttemptBuilder<SchemaT extends KeyspaceSchemaObject> {
    private final int position = 0;
    private final SchemaT schemaObject;

    public ListTablesAttemptBuilder(SchemaT schemaObject) {
      this.schemaObject = schemaObject;
    }

    public ListTablesAttempt build() {
      return new ListTablesAttempt(position, schemaObject);
    }
  }
}
