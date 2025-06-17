package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import java.util.Map;
import java.util.Objects;

public class CreateTypeExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier udtName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public CreateTypeExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement statement, CqlIdentifier udtName) {
    super(schemaObject, statement);
    this.udtName = Objects.requireNonNull(udtName, "udtName must not be null");
  }

  @Override
  public RuntimeException handle(AlreadyExistsException exception) {
    return SchemaException.Code.CANNOT_ADD_EXISTING_TYPE.get(
        Map.of("existingType", errFmt(udtName)));
  }
}
