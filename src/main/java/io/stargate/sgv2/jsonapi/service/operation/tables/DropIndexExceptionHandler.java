package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

import java.util.Map;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

public class DropIndexExceptionHandler extends KeyspaceDriverExceptionHandler{

  private final CqlIdentifier indexName;

  public DropIndexExceptionHandler(CqlIdentifier indexName) {
    this.indexName = indexName;
  }

  @Override
  public RuntimeException handle(
      KeyspaceSchemaObject schemaObject, InvalidQueryException exception) {

    // Need to wait for the keyspace to have the keyspace metadata to get the list of indexes :(
    if (exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.CANNOT_DROP_UNKNOWN_INDEX.get(Map.of("unknownIndex", errFmt(indexName)));
    }
    return super.handle(schemaObject, exception);
  }
}
