package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createType;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.schema.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;

public class CreateTypeDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final ApiUdtType apiUdtType;
  private final boolean ifNotExists;

  protected CreateTypeDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      ApiUdtType apiUdtType,
      boolean ifNotExists) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);

    this.apiUdtType = apiUdtType;
    this.ifNotExists = ifNotExists;

    // sanity check
    if (apiUdtType.allFields().isEmpty()) {
      throw new IllegalArgumentException(
          "CreateTypeDBTask() - apiUdtType.allFields() is empty, must have at least one field");
    }

    setStatus(TaskStatus.READY);
  }

  public static CreateTypeDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new CreateTypeDBTaskBuilder(schemaObject);
  }

  @Override
  protected SimpleStatement buildStatement() {
    var keyspaceIdentifier = schemaObject.identifier().keyspace();

    CreateTypeStart createTypeStart = createType(keyspaceIdentifier, apiUdtType.udtName());

    // Add if not exists flag based on request
    if (ifNotExists) {
      createTypeStart = createTypeStart.ifNotExists();
    }

    CreateType createType = null;
    for (var apiColumnDef : apiUdtType.allFields().values()) {
      createType =
          createType == null
              ? createTypeStart.withField(apiColumnDef.name(), apiColumnDef.type().cqlType())
              : createType.withField(apiColumnDef.name(), apiColumnDef.type().cqlType());
    }

    assert createType != null;
    return createType.build();
  }
}
