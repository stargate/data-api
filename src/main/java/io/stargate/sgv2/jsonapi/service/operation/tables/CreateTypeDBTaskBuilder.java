package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import java.util.Objects;

/** Builds a {@link CreateTypeDBTask}. */
public class CreateTypeDBTaskBuilder
    extends TaskBuilder<CreateTypeDBTask, KeyspaceSchemaObject, CreateTypeDBTaskBuilder> {

  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;
  private boolean ifNotExists;
  private ApiUdtType apiUdtType;

  protected CreateTypeDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public CreateTypeDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }

  public CreateTypeDBTaskBuilder withApiUdtType(ApiUdtType apiUdtType) {
    this.apiUdtType = apiUdtType;
    return this;
  }

  public CreateTypeDBTaskBuilder ifNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateTypeDBTask build() {

    Objects.requireNonNull(apiUdtType, "apiUdtType must not be null");

    return new CreateTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        apiUdtType,
        ifNotExists);
  }
}
