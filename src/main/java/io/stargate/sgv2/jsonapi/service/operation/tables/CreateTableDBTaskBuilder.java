package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import java.util.Map;

/** Builds a {@link CreateTableDBTask}. */
public class CreateTableDBTaskBuilder
    extends TaskBuilder<CreateTableDBTask, KeyspaceSchemaObject, CreateTableDBTaskBuilder> {

  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;

  private Map<String, String> customProperties;
  private boolean ifNotExists;
  private ApiTableDef tableDef;

  protected CreateTableDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public CreateTableDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }

  public CreateTableDBTaskBuilder tableDef(ApiTableDef tableDef) {
    this.tableDef = tableDef;
    return this;
  }

  public CreateTableDBTaskBuilder customProperties(Map<String, String> customProperties) {
    this.customProperties = customProperties;
    return this;
  }

  public CreateTableDBTaskBuilder ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateTableDBTask build() {
    return new CreateTableDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        tableDef,
        ifNotExists,
        customProperties);
  }
}
