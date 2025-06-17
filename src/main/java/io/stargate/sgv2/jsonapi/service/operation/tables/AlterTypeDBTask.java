package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTypeStart;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import java.util.Map;

public class AlterTypeDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final AlterTypeDBTaskType alterTypeDBTaskType;
  private final Map.Entry<CqlIdentifier, CqlIdentifier> renamingFieldEntry;
  private final ApiColumnDef addingField;
  private final CqlIdentifier udtName;

  protected AlterTypeDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      AlterTypeDBTaskType alterTypeDBTaskType,
      Map.Entry<CqlIdentifier, CqlIdentifier> renamingFieldEntry,
      ApiColumnDef addingField,
      CqlIdentifier udtName) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);
    this.alterTypeDBTaskType = alterTypeDBTaskType;
    this.renamingFieldEntry = renamingFieldEntry;
    this.addingField = addingField;
    this.udtName = udtName;
    setStatus(TaskStatus.READY);
  }

  public static AlterTypeDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new AlterTypeDBTaskBuilder(schemaObject);
  }

  @Override
  protected SimpleStatement buildStatement() {
    var keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());

    AlterTypeStart alterTypeStart = alterType(keyspaceIdentifier, udtName);
    return switch (alterTypeDBTaskType) {
      case ADD_FIELD ->
          alterTypeStart.addField(addingField.name(), addingField.type().cqlType()).build();
      case RENAME_FIELD ->
          alterTypeStart
              .renameField(renamingFieldEntry.getKey(), renamingFieldEntry.getValue())
              .build();
    };
  }

  /**
   * Enum representing the types of alterations that can be performed on a user-defined type (UDT).
   */
  public enum AlterTypeDBTaskType {
    ADD_FIELD,
    RENAME_FIELD;
  }
}
