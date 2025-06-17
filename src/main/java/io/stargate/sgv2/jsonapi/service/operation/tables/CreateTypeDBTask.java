package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createType;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtDef;

public class CreateTypeDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final ApiUdtDef udtDef;
  private final boolean ifNotExists;

  protected CreateTypeDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      ApiUdtDef udtDef,
      boolean ifNotExists) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);
    this.udtDef = udtDef;
    this.ifNotExists = ifNotExists;

    setStatus(TaskStatus.READY);
  }

  public static CreateTypeDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new CreateTypeDBTaskBuilder(schemaObject);
  }

  @Override
  protected SimpleStatement buildStatement() {
    var keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());

    CreateTypeStart createTypeStart = createType(keyspaceIdentifier, udtDef.name());

    // Add if not exists flag based on request
    if (ifNotExists) {
      createTypeStart = createTypeStart.ifNotExists();
    }

    CreateType createType = null;
    // Add all fields
    for (var columnDef : udtDef.allFields().values()) {
      createType =
          createType == null
              ? createTypeStart.withField(columnDef.name(), columnDef.type().cqlType())
              : createType.withField(columnDef.name(), columnDef.type().cqlType());
    }
    // note, validation of empty fields has been done.
    return createType.build();
  }
}
