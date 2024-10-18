package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class AlterTableAttemptBuilder {
  private AlterTableType alterTableType;
  private final TableSchemaObject schemaObject;
  private Map<String, ApiDataType> addColumns;
  private List<String> dropColumns;
  private Map<String, String> customProperties;
  private int position = 0;
  private final SchemaAttempt.SchemaRetryPolicy retryPolicy;

  public AlterTableAttemptBuilder(TableSchemaObject schemaObject) {
    this.schemaObject = schemaObject;
    this.retryPolicy = new SchemaAttempt.SchemaRetryPolicy(2, Duration.ofMillis(10));
  }

  public AlterTableAttemptBuilder addColumns(Map<String, ApiDataType> addColumns) {
    assert this.alterTableType == null
        : "Cannot set addColumns when alterTableType is already set as " + this.alterTableType;
    this.addColumns = addColumns;
    if (addColumns != null && !addColumns.isEmpty()) {
      this.alterTableType = AlterTableType.ADD_COLUMNS;
    }
    return this;
  }

  public AlterTableAttemptBuilder dropColumns(List<String> dropColumns) {
    assert this.alterTableType == null
        : "Cannot set dropColumns when alterTableType is already set as " + this.alterTableType;
    this.dropColumns = dropColumns;
    if (dropColumns != null && !dropColumns.isEmpty()) {
      this.alterTableType = AlterTableType.DROP_COLUMNS;
    }
    return this;
  }

  public AlterTableAttemptBuilder customProperties(Map<String, String> customProperties) {
    assert this.alterTableType == null
        : "Cannot set customProperties when alterTableType is already set as "
            + this.alterTableType;
    this.customProperties = customProperties;
    if (customProperties != null && !customProperties.isEmpty()) {
      this.alterTableType = AlterTableType.UPDATE_EXTENSIONS;
    }
    return this;
  }

  // Build method to create an instance of AlterTableAttempt
  public AlterTableAttempt build() {
    return new AlterTableAttempt(
        position++,
        schemaObject,
        alterTableType,
        addColumns,
        dropColumns,
        customProperties,
        retryPolicy);
  }
}
