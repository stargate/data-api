package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import java.util.Map;
import java.util.Objects;

public class DropTypeExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier typeName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public DropTypeExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement simpleStatement, CqlIdentifier typeName) {
    super(schemaObject, simpleStatement);
    this.typeName = Objects.requireNonNull(typeName, "typeName must not be null");
  }

  @Override
  public RuntimeException handle(InvalidQueryException exception) {

    // Example driver message:
    // "Type 'demo.fake' doesn't exist"
    if (exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.CANNOT_DROP_UNKNOWN_TYPE.get(
          Map.of("unknownType", errFmt(typeName)));
    }

    // Example driver message:
    // "Cannot drop user type 'demo.address' as it is still used by tables table_udt, table_udt2"
    if (exception.getMessage().contains("is still used by tables")) {

      var prefix = "is still used by tables";
      var start = exception.getMessage().indexOf(prefix);
      String tableNames;
      if (start > -1) {
        // other errors have " around the names
        tableNames = "\"" + exception.getMessage().substring(start + prefix.length()).trim() + "\"";
      } else {
        tableNames = "unknown tables";
      }

      return SchemaException.Code.CANNOT_DROP_TYPE_USED_BY_TABLE.get(
          "usedType", errFmt(typeName), "tableNames", tableNames);
    }
    return super.handle(exception);
  }
}
