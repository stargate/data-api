package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;

/**
 * Attempt to list tables in a keyspace.
 *
 * @param <SchemaT> The keyspace schema object.
 */
public class ListTablesAttempt<SchemaT extends KeyspaceSchemaObject>
    extends MetadataAttempt<SchemaT> {

  private ListTablesAttempt(int position, SchemaT schemaObject) {
    super(position, schemaObject, new MetadataAttempt.NoRetryPolicy());
    setStatus(OperationStatus.READY);
  }

  /**
   * Get table names from the keyspace metadata.
   *
   * @return List of table names.
   */
  protected List<String> getTableNames() {
    return getTables().stream()
        .map(
            schemaObject ->
                CqlIdentifierUtil.externalRepresentation(schemaObject.tableMetadata().getName()))
        .toList();
  }

  /**
   * Get tables schema for all the tables in the keyspace.
   *
   * @return List of table schema.
   */
  protected List<MetadataAttempt.TableResponse> getTablesSchema() {
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
