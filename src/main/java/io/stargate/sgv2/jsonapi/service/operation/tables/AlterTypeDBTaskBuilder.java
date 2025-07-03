package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;

/**
 * Builds a {@link AlterTypeDBTask}. Note, alterType CQL statement only allows one task one time,
 * I.E. You can only do one field renaming or one field adding at a time. So, the {@link
 * AlterTypeDBTaskBuilder} is used to build a single {@link AlterTypeDBTask} for each task.
 */
public class AlterTypeDBTaskBuilder
    extends TaskBuilder<AlterTypeDBTask, KeyspaceSchemaObject, AlterTypeDBTaskBuilder> {

  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;
  private CqlIdentifier udtName;

  protected AlterTypeDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public AlterTypeDBTaskBuilder withTypeName(CqlIdentifier typeName) {
    this.udtName = typeName;
    return this;
  }

  public AlterTypeDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }

  public AlterTypeDBTask buildForAddField(ApiColumnDef fieldDef) {
    return new AlterTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        AlterTypeDBTask.AlterTypeDBTaskType.ADD_FIELD,
        udtName,
        fieldDef,
        null,
        null);
  }

  public AlterTypeDBTask buildForRenameField(String fromField, String toField) {
    return new AlterTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        AlterTypeDBTask.AlterTypeDBTaskType.RENAME_FIELD,
        udtName,
        null,
        cqlIdentifierFromUserInput(fromField),
        cqlIdentifierFromUserInput(toField));
  }
}
