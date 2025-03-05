package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiRegularIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndex;
import java.util.Objects;

/** Builder for a {@link CreateIndexDBTask}. */
public class CreateIndexDBTaskBuilder
    extends TaskBuilder<CreateIndexDBTask, TableSchemaObject, CreateIndexDBTaskBuilder> {

  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;
  // must be specified, the default should not be defined in here
  private Boolean ifNotExists = null;

  protected CreateIndexDBTaskBuilder(TableSchemaObject schemaObject) {
    super(schemaObject);
  }

  public CreateIndexDBTaskBuilder withIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateIndexDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }

  private void checkBuildPreconditions() {
    Objects.requireNonNull(ifNotExists, "ifNotExists cannot be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
  }

  private CQLOptions.CreateIndexStartCQLOptions buildCqlOptions() {
    var cqlOptions = new CQLOptions.CreateIndexStartCQLOptions();
    if (ifNotExists) {
      cqlOptions.addBuilderOption(CQLOption.ForCreateIndexStart.ifNotExists(true));
    }
    return cqlOptions;
  }

  public CreateIndexDBTask build(ApiRegularIndex apiRegularIndex) {
    Objects.requireNonNull(apiRegularIndex, "apiRegularIndex cannot be null");
    checkBuildPreconditions();

    return new CreateIndexDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        apiRegularIndex,
        buildCqlOptions());
  }

  public CreateIndexDBTask build(ApiVectorIndex apiVectorIndex) {
    Objects.requireNonNull(apiVectorIndex, "apiVectorIndex cannot be null");
    checkBuildPreconditions();

    return new CreateIndexDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        apiVectorIndex,
        buildCqlOptions());
  }
}
