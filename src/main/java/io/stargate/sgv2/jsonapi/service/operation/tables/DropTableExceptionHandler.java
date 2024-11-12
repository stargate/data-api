package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

import java.util.Map;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

public class DropTableExceptionHandler extends KeyspaceDriverExceptionHandler{

  private final CqlIdentifier tableName;

  public DropTableExceptionHandler(CqlIdentifier tableName) {
    this.tableName = tableName;
  }
  
  @Override
  public RuntimeException handle(
      KeyspaceSchemaObject schemaObject, InvalidQueryException exception) {

    // Need to wait for keyspace to have keyspace metadata to get the list of tables :(
    if (exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.CANNOT_DROP_UNKNOWN_TABLE.get(Map.of("unknownTable", errFmt(tableName)));
    }
    return super.handle(schemaObject, exception);
  }
}
