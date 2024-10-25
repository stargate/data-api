package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKeyDesc;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Builds a {@link CreateTableAttempt}. */
public class CreateTableAttemptBuilder {
  private int position;
  private KeyspaceSchemaObject schemaObject;
  private int retryDelayMillis;
  private int maxRetries;
  private String tableName;
  private Map<CqlIdentifier, ApiDataType> columnTypes;
  private List<CqlIdentifier> partitionKeys;
  private List<PrimaryKeyDesc.OrderingKeyDesc> clusteringKeys;
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
    this.columnTypes =
        columnTypes.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> CqlIdentifierUtil.cqlIdentifierFromUserInput(e.getKey()),
                    Map.Entry::getValue));
    return this;
  }

  public CreateTableAttemptBuilder partitionKeys(List<String> partitionKeys) {
    this.partitionKeys =
        partitionKeys.stream().map(CqlIdentifierUtil::cqlIdentifierFromUserInput).toList();
    ;
    return this;
  }

  public CreateTableAttemptBuilder clusteringKeys(
      List<PrimaryKeyDesc.OrderingKeyDesc> clusteringKeys) {
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
