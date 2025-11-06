package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.Map;
import java.util.Objects;

/** Exception handler for the {@link CreateIndexDBTask} */
public class CreateIndexExceptionHandler extends TableDriverExceptionHandler {

  private final CqlIdentifier indexName;

  /** Compatible with {@link FactoryWithIdentifier} */
  public CreateIndexExceptionHandler(
      RequestContext requestContext,
      TableSchemaObject schemaObject,
      SimpleStatement statement,
      CQLSessionCache sessionCache,
      CqlIdentifier indexName) {
    super(requestContext, schemaObject, statement, sessionCache);
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
  }

  @Override
  public RuntimeException handle(InvalidQueryException exception) {

    // AlreadyExistsException is only used for tables and keyspaces, so have to still do a string
    // test :(

    // Need to wait for the keyspace to have the keyspace metadata to get the list of indexes :(
    if (exception.getMessage().contains("already exists")) {
      return SchemaException.Code.CANNOT_ADD_EXISTING_INDEX.get(
          Map.of("existingIndex", errFmt(indexName)));
    }
    return super.handle(exception);
  }
}
