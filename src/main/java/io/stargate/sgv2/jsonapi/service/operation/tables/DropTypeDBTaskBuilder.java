package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import java.util.Objects;

/** Builds a {@link DropTypeDBTask}. */
public class DropTypeDBTaskBuilder
    extends TaskBuilder<DropTypeDBTask, KeyspaceSchemaObject, DropTypeDBTaskBuilder> {

  private CqlIdentifier name = null;
  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy = null;

  // must be specified, the default should not be defined in here
  private Boolean ifExists = null;

  private final CQLOptions.BuildableCQLOptions<Drop> cqlOptions =
      new CQLOptions.BuildableCQLOptions<>();

  protected DropTypeDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public DropTypeDBTaskBuilder withTypeName(CqlIdentifier typeName) {
    this.name = Objects.requireNonNull(typeName, "typeName must not be null");
    return this;
  }

  public DropTypeDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy =
        Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    return this;
  }

  public DropTypeDBTaskBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropTypeDBTask build() {
    Objects.requireNonNull(name, "tableName must not be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
    Objects.requireNonNull(ifExists, "ifExists cannot be null");

    if (ifExists) {
      cqlOptions.addBuilderOption(CQLOption.ForDrop.ifExists());
    }

    return new DropTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        name,
        cqlOptions);
  }
}
