package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;

/**
 * Subclass of {@link DefaultDriverExceptionHandler} for working with {@link KeyspaceSchemaObject}.
 *
 * <p>The class may be used directly when working with a Table and there are no specific exception
 * handling for the command, or it may be subclassed by exception handlers for a command that have
 * specific exception handling such as for a table already exists exception.
 */
public class KeyspaceDriverExceptionHandler
    extends DefaultDriverExceptionHandler<KeyspaceSchemaObject> {

  public KeyspaceDriverExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement statement) {
    super(schemaObject, statement);
  }
}
