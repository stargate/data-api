package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Builder for a {@link AlterTableDBTask}. */
public class AlterTableDBTaskBuilder
    extends TaskBuilder<AlterTableDBTask, TableSchemaObject, AlterTableDBTaskBuilder> {

  private TaskRetryPolicy retryPolicy = null;

  protected AlterTableDBTaskBuilder(TableSchemaObject schemaObject) {
    super(schemaObject);
  }

  public AlterTableDBTaskBuilder withRetryPolicy(TaskRetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  private void checkRequirements() {
    Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
  }

  public AlterTableDBTask buildAddColumns(ApiColumnDefContainer addColumns) {

    checkRequirements();
    return new AlterTableDBTask(
        nextPosition(),
        schemaObject,
        getExceptionHandlerFactory(),
        AlterTableDBTask.AlterTableType.ADD_COLUMNS,
        addColumns,
        null,
        null,
        retryPolicy);
  }

  public AlterTableDBTask buildDropColumns(List<CqlIdentifier> dropColumns) {

    checkRequirements();
    return new AlterTableDBTask(
        nextPosition(),
        schemaObject,
        getExceptionHandlerFactory(),
        AlterTableDBTask.AlterTableType.DROP_COLUMNS,
        null,
        dropColumns,
        null,
        retryPolicy);
  }

  public AlterTableDBTask buildUpdateExtensions(Map<String, String> customProperties) {

    checkRequirements();
    return new AlterTableDBTask(
        nextPosition(),
        schemaObject,
        getExceptionHandlerFactory(),
        AlterTableDBTask.AlterTableType.UPDATE_EXTENSIONS,
        null,
        null,
        customProperties,
        retryPolicy);
  }
}
