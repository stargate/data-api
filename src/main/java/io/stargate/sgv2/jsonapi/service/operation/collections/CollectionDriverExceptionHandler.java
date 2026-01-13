package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.ws.rs.core.Response;

import java.util.List;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

/**
 * Subclass of {@link DefaultDriverExceptionHandler} for working with {@link
 * io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject}.
 *
 * <p>The class may be used directly when working with a Collection and there are no specific
 * exception handling for the command, or it may be subclassed by exception handlers for a command
 * that have specific exception handling such as for {@link CreateIndexExceptionHandler} (for a
 * table)
 */
public class CollectionDriverExceptionHandler
    extends DefaultDriverExceptionHandler<CollectionSchemaObject> {

  private static final List<String> CORRUPTED_COLLECTION_MESSAGES = List.of(
      "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING",
      "ANN ordering by vector requires the column to be indexed",
      "Invalid analyzer config" // Lexical problem
  );

  public CollectionDriverExceptionHandler(
      CollectionSchemaObject schemaObject, SimpleStatement statement) {
    super(schemaObject, statement);
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  /**
   * Overriding for some specific issues with collections, then fallback
   */
  @Override
  public RuntimeException handle(QueryValidationException exception) {

    for (var msg : CORRUPTED_COLLECTION_MESSAGES) {
      if (exception.getMessage().contains(msg)) {
        return DatabaseException.Code.CORRUPTED_COLLECTION_SCHEMA.get(
            errVars(schemaObject, exception)
        );
      }
    }

    // [data-api#2068]: Need to convert Lexical-value-too-big failure to something more meaningful
    // XXX TODO: how to get the actual size ? https://github.com/stargate/data-api/issues/2068
    if (exception.getMessage().contains(
        "analyzed size for column query_lexical_value exceeds the cumulative limit for index")) {
      return DocumentException.Code.LEXICAL_CONTENT_TOO_LONG.get(
          errVars(schemaObject, exception)
      );
    }
    return super.handle(exception);
  }
}
