package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

import java.util.Map;
import java.util.Objects;

public class DropIndexExceptionHandler extends KeyspaceDriverExceptionHandler {

  private final CqlIdentifier indexName;

  /**
   * Compatible with {@link FactoryWithIdentifier}
   */
  public DropIndexExceptionHandler(KeyspaceSchemaObject schemaObject, SimpleStatement statement, CqlIdentifier indexName) {
    super(schemaObject, statement);
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
  }

  @Override
  public RuntimeException handle(InvalidQueryException exception) {

    // Need to wait for the keyspace to have the keyspace metadata to get the list of indexes :(
    if (exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.CANNOT_DROP_UNKNOWN_INDEX.get(
          Map.of("unknownIndex", errFmt(indexName)));
    }
    return super.handle(exception);
  }
}
