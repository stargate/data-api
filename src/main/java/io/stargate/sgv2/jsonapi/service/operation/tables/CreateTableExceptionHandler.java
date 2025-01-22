package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

import java.util.Map;
import java.util.Objects;

/**
 * Exception handler for the {@link CreateTableAttempt}
 */
public class CreateTableExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier tableName;

  /**
   * Compatible with {@link FactoryWithIdentifier}
   */
  public CreateTableExceptionHandler(KeyspaceSchemaObject schemaObject, SimpleStatement statement, CqlIdentifier tableName) {
    super(schemaObject, statement);
    this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
  }

  @Override
  public RuntimeException handle(AlreadyExistsException exception) {

    if (exception.getMessage().contains("already exists")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_TABLE.get(
          Map.of("existingTable", errFmt(tableName)));
    }
    return super.handle(exception);
  }
}
