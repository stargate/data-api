package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import java.util.Map;

public class CreateTableExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier tableName;

  public CreateTableExceptionHandler(CqlIdentifier tableName) {
    this.tableName = tableName;
  }

  @Override
  public RuntimeException handle(
      KeyspaceSchemaObject schemaObject, AlreadyExistsException exception) {

    if (exception.getMessage().contains("already exists")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_TABLE.get(
          Map.of("existingTable", errFmt(tableName)));
    }
    return super.handle(schemaObject, exception);
  }
}
