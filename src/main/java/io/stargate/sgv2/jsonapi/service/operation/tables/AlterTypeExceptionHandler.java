package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import java.util.Map;
import java.util.Objects;

public class AlterTypeExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier udtName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public AlterTypeExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement statement, CqlIdentifier udtName) {
    super(schemaObject, statement);
    this.udtName = Objects.requireNonNull(udtName, "udtName must not be null");
  }

  @Override
  public RuntimeException handle(InvalidQueryException exception) {
    // Note, "Unkown" is a type in driver.
    if (exception.getMessage().contains("Unkown field")) {
      return SchemaException.Code.CANNOT_RENAME_UNKNOWN_TYPE_FIELD.get(
          Map.of("typeName", errFmt(udtName), "driverMessage", exception.getMessage()));
    }
    if (exception.getMessage().contains("already exists")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_FIELD.get(
          Map.of("typeName", errFmt(udtName), "driverMessage", exception.getMessage()));
    }
    return super.handle(exception);
  }
}
