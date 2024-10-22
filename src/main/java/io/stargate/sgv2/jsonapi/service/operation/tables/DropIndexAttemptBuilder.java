package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builds a {@link DropIndexAttempt}. */
public class DropIndexAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private final CqlIdentifier name;
  private boolean ifExists;
  private final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;

  public DropIndexAttemptBuilder(
      KeyspaceSchemaObject schemaObject,
      String indexName,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaObject = schemaObject;
    this.schemaRetryPolicy = schemaRetryPolicy;
    this.name = CqlIdentifierUtil.cqlIdentifierFromUserInput(indexName);
  }

  public DropIndexAttemptBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropIndexAttempt build() {
    return new DropIndexAttempt(position++, schemaObject, name, ifExists, schemaRetryPolicy);
  }
}
