package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import java.util.Objects;

/** Builds a {@link DropIndexDBTask}. */
public class DropIndexDBTaskBuilder extends TaskBuilder<DropIndexDBTask, KeyspaceSchemaObject> {

  private final CQLOptions.BuildableCQLOptions<Drop> cqlOptions =
      new CQLOptions.BuildableCQLOptions<>();

  private CqlIdentifier indexName;
  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;

  // must be specified, the default should not be defined in here
  private Boolean ifExists = null;

  protected DropIndexDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public DropIndexDBTaskBuilder withIndexName(CqlIdentifier indexName) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    return this;
  }

  public DropIndexDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy =
        Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    return this;
  }

  public DropIndexDBTaskBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropIndexDBTask build() {

    Objects.requireNonNull(indexName, "indexName must not be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    Objects.requireNonNull(ifExists, "ifExists cannot be null");

    if (ifExists) {
      cqlOptions.addBuilderOption(CQLOption.ForDrop.ifExists());
    }
    return new DropIndexDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        indexName,
        cqlOptions);
  }
}
