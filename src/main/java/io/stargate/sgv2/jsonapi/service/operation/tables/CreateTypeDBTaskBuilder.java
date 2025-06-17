package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtDef;

/** Builds a {@link CreateTypeDBTask}. */
public class CreateTypeDBTaskBuilder
    extends TaskBuilder<CreateTypeDBTask, KeyspaceSchemaObject, CreateTypeDBTaskBuilder> {

  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;
  private boolean ifNotExists;
  private ApiUdtDef udtDef;

  protected CreateTypeDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public CreateTypeDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }

  public CreateTypeDBTaskBuilder udtDef(ApiUdtDef udtDef) {
    this.udtDef = udtDef;
    return this;
  }

  public CreateTypeDBTaskBuilder ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateTypeDBTask build() {
    return new CreateTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        udtDef,
        ifNotExists);
  }
}
