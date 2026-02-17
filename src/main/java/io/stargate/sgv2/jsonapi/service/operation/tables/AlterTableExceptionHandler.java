package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.Map;
import java.util.Objects;

/** Exception handler for the {@link AlterTableDBTask} */
public class AlterTableExceptionHandler extends TableDriverExceptionHandler {

  private final CqlIdentifier tableName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public AlterTableExceptionHandler(
      TableSchemaObject schemaObject, SimpleStatement statement, CqlIdentifier tableName) {
    super(schemaObject, statement);
    this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
  }

  /**
   * Handles {@link InvalidQueryException}
   *
   * <ul>
   *   <li>If the message contains "Unknown type", it indicates an error for trying to alter a table
   *       with unknown user defined type (UDT).
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
