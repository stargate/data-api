package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** Exception handler for the {@link AlterTableDBTask} */
public class AlterTableExceptionHandler extends TableDriverExceptionHandler {

  private static final Pattern PREVIOUSLY_DROPPED_COLUMN_PATTERN =
      Pattern.compile(
          "Cannot re-add (?:a )?previously dropped column '([^']+)' of type (.+), incompatible with previous type (.+)");

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
   *   <li>If the message contains "Cannot re-add a previously dropped column", it indicates an
   *       error for trying to add a previously dropped column with a different underlying type.
   * </ul>
   */
  @Override
  public RuntimeException handle(InvalidQueryException exception) {
    if (exception.getMessage().contains("Unknown type")) {
      return SchemaException.Code.UNKNOWN_USER_DEFINED_TYPE.get(
          Map.of("driverMessage", exception.getMessage()));
    }

    var previouslyDroppedColumn = PREVIOUSLY_DROPPED_COLUMN_PATTERN.matcher(exception.getMessage());
    if (previouslyDroppedColumn.matches()) {
      return SchemaException.Code.CANNOT_ADD_PREVIOUSLY_DROPPED_COLUMN.get(
          errVars(
              schemaObject,
              map -> {
                map.put("columnName", previouslyDroppedColumn.group(1));
                map.put("columnType", previouslyDroppedColumn.group(2));
                map.put("previousType", previouslyDroppedColumn.group(3));
              }));
    }
    return exception;
  }
}
