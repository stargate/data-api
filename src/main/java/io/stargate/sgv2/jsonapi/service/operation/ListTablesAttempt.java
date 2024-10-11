package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

import java.util.List;

public class ListTablesAttempt<SchemaT extends KeyspaceSchemaObject>
    extends MetadataAttempt<KeyspaceSchemaObject> {

  private ListTablesAttempt(
      int position, KeyspaceSchemaObject schemaObject, RetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
  }

  /**
   * Get table names from the keyspace metadata.
   *
   * @return
   */
  protected List<String> getTableNames() {
    return getTables().stream()
            .map(
                    schemaObject ->
                            CqlIdentifierUtil.externalRepresentation(schemaObject.tableMetadata().getName()))
            .toList();
  }

  protected List<TableResponse> getTablesSchema() {
    return getTables().stream().map(this::getTableSchema).toList();
  }


  public static class ListTablesAttemptBuilder<SchemaT extends KeyspaceSchemaObject> {
    private final int position = 0;
    private final SchemaT schemaObject;
    private final RetryPolicy retryPolicy;

    public ListTablesAttemptBuilder(SchemaT schemaObject, RetryPolicy retryPolicy) {
      this.schemaObject = schemaObject;
      this.retryPolicy = retryPolicy;
    }

    public ListTablesAttempt build() {
      return new ListTablesAttempt(position, schemaObject, retryPolicy);
    }
  }
}
