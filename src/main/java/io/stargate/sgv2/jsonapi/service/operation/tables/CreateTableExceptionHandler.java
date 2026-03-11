package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import java.util.Map;
import java.util.Objects;

/** Exception handler for the {@link CreateTableDBTask} */
public class CreateTableExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier tableName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public CreateTableExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement statement, CqlIdentifier tableName) {
    super(schemaObject, statement);
    this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
  }

  @Override
  public Throwable handle(AlreadyExistsException exception) {
    return SchemaException.Code.CANNOT_ADD_EXISTING_TABLE.get(
        Map.of("existingTable", errFmt(tableName)));
  }

  /**
   * Handles {@link InvalidQueryException}
   *
   * <ul>
   *   <li>If the message contains "Unknown type", it indicates an error for trying to create a
   *       table with unknown user defined type (UDT).
   * </ul>
   */
  @Override
  public Throwable handle(InvalidQueryException exception) {
    if (exception.getMessage().contains("Unknown type")) {
      return SchemaException.Code.UNKNOWN_USER_DEFINED_TYPE.get(
          Map.of("driverMessage", exception.getMessage()));
    }
    return exception;
  }
}
