package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;import java.util.Map;
import java.util.Objects;

public class DropTableExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier tableName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public DropTableExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement simpleStatement, CqlIdentifier tableName) {
    super(schemaObject, simpleStatement);
    this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
  }

  @Override
  public RuntimeException handle(InvalidQueryException exception) {

    // Need to wait for keyspace to have keyspace metadata to get the list of tables to be included
    // in the message
    if (exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.CANNOT_DROP_UNKNOWN_TABLE.get(
          Map.of("unknownTable", errFmt(tableName)));
    }
    return super.handle(exception);
  }
}
