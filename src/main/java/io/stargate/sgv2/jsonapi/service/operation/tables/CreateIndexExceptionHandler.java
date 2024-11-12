package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.Map;

public class CreateIndexExceptionHandler extends TableDriverExceptionHandler {

  private final CqlIdentifier indexName;

  public CreateIndexExceptionHandler(CqlIdentifier indexName) {
    this.indexName = indexName;
  }

  @Override
  public RuntimeException handle(TableSchemaObject schemaObject, InvalidQueryException exception) {

    // AlreadyExistsException is only used for tables and keyspaces :(

    // Need to wait for the keyspace to have the keyspace metadata to get the list of indexes :(
    if (exception.getMessage().contains("already exists")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_INDEX.get(
          Map.of("existingIndex", errFmt(indexName)));
    }
    return super.handle(schemaObject, exception);
  }
}
