package io.stargate.sgv2.jsonapi.service.operation.databases;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;

/**
 * Subclass of {@link DefaultDriverExceptionHandler} for working with {@link DatabaseSchemaObject}.
 */
public class DatabaseDriverExceptionHandler
    extends DefaultDriverExceptionHandler<DatabaseSchemaObject> {

  public DatabaseDriverExceptionHandler(
      DatabaseSchemaObject schemaObject, SimpleStatement statement) {
    super(schemaObject, statement);
  }
}
