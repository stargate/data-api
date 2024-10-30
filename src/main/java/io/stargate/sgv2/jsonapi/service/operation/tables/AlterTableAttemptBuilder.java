package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import java.util.List;
import java.util.Map;

/** Builder for a {@link AlterTableAttempt}. */
public class AlterTableAttemptBuilder {

  private final TableSchemaObject schemaObject;
  private final SchemaAttempt.SchemaRetryPolicy retryPolicy;
  // zero based counting
  private int position = 0;

  public AlterTableAttemptBuilder(
      TableSchemaObject schemaObject, SchemaAttempt.SchemaRetryPolicy retryPolicy) {
    this.schemaObject = schemaObject;
    this.retryPolicy = retryPolicy;
  }

  public AlterTableAttempt buildAddColumns(ApiColumnDefContainer addColumns) {

    return new AlterTableAttempt(
        position++,
        schemaObject,
        AlterTableAttempt.AlterTableType.ADD_COLUMNS,
        addColumns,
        null,
        null,
        retryPolicy);
  }

  public AlterTableAttempt buildDropColumns(List<CqlIdentifier> dropColumns) {

    return new AlterTableAttempt(
        position++,
        schemaObject,
        AlterTableAttempt.AlterTableType.DROP_COLUMNS,
        null,
        dropColumns,
        null,
        retryPolicy);
  }

  public AlterTableAttempt buildUpdateExtensions(Map<String, String> customProperties) {
    return new AlterTableAttempt(
        position++,
        schemaObject,
        AlterTableAttempt.AlterTableType.UPDATE_EXTENSIONS,
        null,
        null,
        customProperties,
        retryPolicy);
  }
}
