package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

public class ListTablesAttempt extends MetadataAttempt<KeyspaceSchemaObject> {

  public ListTablesAttempt(
      int position, KeyspaceSchemaObject schemaObject, RetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
  }

  public void getNames() {}
}
