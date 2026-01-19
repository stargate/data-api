package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass of {@link DefaultDriverExceptionHandler} for working with {@link KeyspaceSchemaObject}.
 *
 * <p>The class may be used directly when working with a Table and there are no specific exception
 * handling for the command, or it may be subclassed by exception handlers for a command that have
 * specific exception handling such as for a table already exists exception.
 */
public class KeyspaceDriverExceptionHandler
    extends DefaultDriverExceptionHandler<KeyspaceSchemaObject> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KeyspaceDriverExceptionHandler.class);

  public KeyspaceDriverExceptionHandler(
      KeyspaceSchemaObject schemaObject, SimpleStatement statement) {
    super(schemaObject, statement);
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  @Override
  public RuntimeException handle(InvalidQueryException exception) {

    // [data-api#1900]: Need to convert Lexical-index creation failure to something more meaningful
    if (exception.getMessage().contains("Invalid analyzer config")) {
      return SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS.get(
          "message", exception.getMessage());
    }

    return super.handle(exception);
  }
}
