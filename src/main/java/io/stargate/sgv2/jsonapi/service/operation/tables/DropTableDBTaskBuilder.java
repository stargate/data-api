package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import java.util.Objects;

/** Builds a {@link DropTableDBTask}. */
public class DropTableDBTaskBuilder
    extends TaskBuilder<DropTableDBTask, KeyspaceSchemaObject, DropTableDBTaskBuilder> {

  private CqlIdentifier name = null;
  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy = null;

  // must be specified, the default should not be defined in here
  private Boolean ifExists = null;

  private final CQLOptions.BuildableCQLOptions<Drop> cqlOptions =
      new CQLOptions.BuildableCQLOptions<>();

  protected DropTableDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public DropTableDBTaskBuilder withTableName(CqlIdentifier tableName) {
    this.name = Objects.requireNonNull(tableName, "tableName must not be null");
    return this;
  }

  public DropTableDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy =
        Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    return this;
  }

  public DropTableDBTaskBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropTableDBTask build() {
    Objects.requireNonNull(name, "tableName must not be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    Objects.requireNonNull(ifExists, "ifExists cannot be null");

    if (ifExists) {
      cqlOptions.addBuilderOption(CQLOption.ForDrop.ifExists());
    }

    return new DropTableDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        name,
        cqlOptions);
  }
}
