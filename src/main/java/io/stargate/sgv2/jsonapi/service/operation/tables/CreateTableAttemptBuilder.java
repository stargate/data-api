package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.util.List;
import java.util.Map;

/** Builds a {@link CreateTableAttempt}. */
public class CreateTableAttemptBuilder {
  private int position;
  private KeyspaceSchemaObject schemaObject;
  private int retryDelayMillis;
  private int maxRetries;
  private String tableName;
  private Map<String, ApiDataType> columnTypes;
  private List<String> partitionKeys;
  private List<PrimaryKey.OrderingKey> clusteringKeys;
  private Map<String, String> customProperties;
  private boolean ifNotExists;

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

  public CreateTableAttemptBuilder tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public CreateTableAttemptBuilder columnTypes(Map<String, ApiDataType> columnTypes) {
    this.columnTypes = columnTypes;
    return this;
  }

  public CreateTableAttemptBuilder partitionKeys(List<String> partitionKeys) {
    this.partitionKeys = partitionKeys;
    return this;
  }

  public CreateTableAttemptBuilder clusteringKeys(List<PrimaryKey.OrderingKey> clusteringKeys) {
    this.clusteringKeys = clusteringKeys;
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
        tableName,
        columnTypes,
        partitionKeys,
        clusteringKeys,
        ifNotExists,
        customProperties);
  }
}
