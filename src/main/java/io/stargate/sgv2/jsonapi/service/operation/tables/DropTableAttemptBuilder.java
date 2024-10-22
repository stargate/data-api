package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builds a {@link DropTableAttempt}. */
public class DropTableAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private final CqlIdentifier name;
  private boolean ifExists;
  private final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;

  public DropTableAttemptBuilder(
      KeyspaceSchemaObject schemaObject,
      String tableName,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaObject = schemaObject;
    this.schemaRetryPolicy = schemaRetryPolicy;
    this.name = CqlIdentifierUtil.cqlIdentifierFromUserInput(tableName);
  }

  public DropTableAttemptBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropTableAttempt build() {
    return new DropTableAttempt(position++, schemaObject, name, ifExists, schemaRetryPolicy);
  }
}
