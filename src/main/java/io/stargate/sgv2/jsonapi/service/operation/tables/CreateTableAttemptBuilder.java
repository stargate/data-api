package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import java.util.Map;

/** Builds a {@link CreateTableAttempt}. */
public class CreateTableAttemptBuilder {
  private int position;
  private KeyspaceSchemaObject schemaObject;
  private int retryDelayMillis;
  private int maxRetries;
  private Map<String, String> customProperties;
  private boolean ifNotExists;
  private ApiTableDef tableDef;

  public CreateTableAttemptBuilder(int position, KeyspaceSchemaObject schemaObject) {
    this.position = position;
    this.schemaObject = schemaObject;
  }

  // Builder methods to set individual fields
  public CreateTableAttemptBuilder retryDelayMillis(int retryDelayMillis) {
    this.retryDelayMillis = retryDelayMillis;
    return this;
  }

  public CreateTableAttemptBuilder maxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public CreateTableAttemptBuilder tableDef(ApiTableDef tableDef) {
    this.tableDef = tableDef;
    return this;
  }

  public CreateTableAttemptBuilder customProperties(Map<String, String> customProperties) {
    this.customProperties = customProperties;
    return this;
  }

  public CreateTableAttemptBuilder ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  // Build method to create an instance of CreateTableAttempt
  public CreateTableAttempt build() {
    return new CreateTableAttempt(
        position++,
        schemaObject,
        retryDelayMillis,
        maxRetries,
        tableDef,
        ifNotExists,
        customProperties);
  }
}
