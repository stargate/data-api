package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds an attempt to truncate a table. */
public class TruncateAttemptBuilder<SchemaT extends TableSchemaObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TruncateAttemptBuilder.class);

  // first value is zero, but we increment before we use it
  private int attemptPosition = -1;

  private final SchemaT tableBasedSchema;

  /**
   * @param tableBasedSchema Table based schema object
   */
  public TruncateAttemptBuilder(SchemaT tableBasedSchema) {
    this.tableBasedSchema = tableBasedSchema;
  }

  public TruncateAttempt<SchemaT> build() {

    attemptPosition += 1;
    return new TruncateAttempt<>(attemptPosition, tableBasedSchema);
  }
}
