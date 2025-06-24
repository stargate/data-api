package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import java.util.Map;

/**
 * Builds a {@link AlterTypeDBTask}. Note, alterType CQL statement only allows one task one time,
 * I.E. You can only do one field renaming or one field adding at a time. So, the {@link
 * AlterTypeDBTaskBuilder} is used to build a single {@link AlterTypeDBTask} for each task.
 */
public class AlterTypeDBTaskBuilder
    extends TaskBuilder<AlterTypeDBTask, KeyspaceSchemaObject, AlterTypeDBTaskBuilder> {

  private SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy;
  private Map.Entry<CqlIdentifier, CqlIdentifier> renamingFieldEntry;
  private ApiColumnDef addingField;
  private CqlIdentifier typeName;

  protected AlterTypeDBTaskBuilder(KeyspaceSchemaObject schemaObject) {
    super(schemaObject);
  }

  public AlterTypeDBTask buildForAddingField() {
    return new AlterTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        AlterTypeDBTask.AlterTypeDBTaskType.ADD_FIELD,
        null,
        addingField,
        typeName);
  }

  public AlterTypeDBTask buildForRenamingField() {
    return new AlterTypeDBTask(
        nextPosition(),
        schemaObject,
        schemaRetryPolicy,
        getExceptionHandlerFactory(),
        AlterTypeDBTask.AlterTypeDBTaskType.RENAME_FIELD,
        renamingFieldEntry,
        null,
        typeName);
  }

  public AlterTypeDBTaskBuilder withRenamingField(
      Map.Entry<CqlIdentifier, CqlIdentifier> renamingFieldEntry) {
    this.renamingFieldEntry = renamingFieldEntry;
    return this;
  }

  public AlterTypeDBTaskBuilder withAddingField(ApiColumnDef fieldDef) {
    this.addingField = fieldDef;
    return this;
  }

  public AlterTypeDBTaskBuilder withTypeName(CqlIdentifier typeName) {
    this.typeName = typeName;
    return this;
  }

  public AlterTypeDBTaskBuilder withSchemaRetryPolicy(
      SchemaDBTask.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }
}
