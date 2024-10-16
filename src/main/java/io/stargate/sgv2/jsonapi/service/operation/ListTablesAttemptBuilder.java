package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

/** */
public class ListTablesAttemptBuilder {
  private int position = 0;
  private final KeyspaceSchemaObject schemaObject;

  public ListTablesAttemptBuilder(KeyspaceSchemaObject schemaObject) {
    this.schemaObject = schemaObject;
  }

  public ListTablesAttempt build() {
    return new ListTablesAttempt(position++, schemaObject);
  }
}
