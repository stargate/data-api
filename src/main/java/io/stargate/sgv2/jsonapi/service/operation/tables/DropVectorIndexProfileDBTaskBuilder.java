package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import java.util.Map;
import java.util.Objects;

/** Builds a {@link DropVectorIndexProfileDBTask}. */
public class DropVectorIndexProfileDBTaskBuilder
    extends TaskBuilder<
        DropVectorIndexProfileDBTask, KeyspaceSchemaObject, DropVectorIndexProfileDBTaskBuilder> {

  private CqlIdentifier tableName;
  private Map<String, String> customProperties;
  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;

  protected DropVectorIndexProfileDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public DropVectorIndexProfileDBTaskBuilder withTableName(CqlIdentifier tableName) {
    this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
    return this;
  }

  public DropVectorIndexProfileDBTaskBuilder withCustomProperties(
      Map<String, String> customProperties) {
    this.customProperties =
        Objects.requireNonNull(customProperties, "customProperties must not be null");
    return this;
  }

  public DropVectorIndexProfileDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy =
        Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    return this;
  }

  public DropVectorIndexProfileDBTask build() {

    Objects.requireNonNull(tableName, "tableName must not be null");
    Objects.requireNonNull(customProperties, "customProperties must not be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");

    return new DropVectorIndexProfileDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        tableName,
        customProperties);
  }
}
