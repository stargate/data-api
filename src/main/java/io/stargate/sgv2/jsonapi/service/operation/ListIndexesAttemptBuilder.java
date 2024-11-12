package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** Builds a {@link ListIndexesAttempt}. */
public class ListIndexesAttemptBuilder {
  private int position = 0;
  private final TableSchemaObject schemaObject;

  public ListIndexesAttemptBuilder(TableSchemaObject schemaObject) {
    this.schemaObject = schemaObject;
  }

  public ListIndexesAttempt build() {
    return new ListIndexesAttempt(position++, schemaObject);
  }
}
