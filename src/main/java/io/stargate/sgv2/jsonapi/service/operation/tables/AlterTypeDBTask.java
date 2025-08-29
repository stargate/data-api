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
import java.util.Objects;

public class AlterTypeDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final AlterTypeDBTaskType alterTypeDBTaskType;
  private final CqlIdentifier udtName;
  private final ApiColumnDef fieldToAdd;
  private final CqlIdentifier fromFieldName;
  private final CqlIdentifier toFieldName;

  protected AlterTypeDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      AlterTypeDBTaskType alterTypeDBTaskType,
      CqlIdentifier udtName,
      ApiColumnDef fieldToAdd,
      CqlIdentifier fromFieldName,
      CqlIdentifier toFieldName) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);

    this.alterTypeDBTaskType = alterTypeDBTaskType;
    this.udtName = udtName;
    this.fieldToAdd = fieldToAdd;
    this.fromFieldName = fromFieldName;
    this.toFieldName = toFieldName;

    // sanity checks
    switch (alterTypeDBTaskType) {
      case ADD_FIELD -> {
        Objects.requireNonNull(
            fieldToAdd, "fieldToAdd must not be null for task type " + alterTypeDBTaskType);
      }
      case RENAME_FIELD -> {
        Objects.requireNonNull(
            fromFieldName, "fromFieldName must not be null for task type " + alterTypeDBTaskType);
        Objects.requireNonNull(
            toFieldName, "toFieldName must not be null for task type " + alterTypeDBTaskType);
      }
    }
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
          alterTypeStart.addField(fieldToAdd.name(), fieldToAdd.type().cqlType()).build();
      case RENAME_FIELD -> alterTypeStart.renameField(fromFieldName, toFieldName).build();
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
