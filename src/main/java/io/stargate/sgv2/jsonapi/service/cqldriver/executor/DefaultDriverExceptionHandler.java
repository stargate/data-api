package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Default implementation of the {@link DriverExceptionHandler} interface, we keep the interface so
 * all the type casting is done in one place and this class only worries about processing the
 * errors.
 *
 * <p>This class should cover almost all the driver exceptions, we create subclasses like the {@link
 * io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler} to handle errors
 * in a table specific (or other schema object) way. e.g. if we have different error codes for a
 * timeout for a table and a collection.
 *
 * <p><b>NOTE:</b> Try to keep the <code>handle()</code> functions grouped like they are in the
 * interface.
 *
 * @param <SchemaT> The type of schema object this handler is for.
 */
public class DefaultDriverExceptionHandler<SchemaT extends SchemaObject>
    implements DriverExceptionHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultDriverExceptionHandler.class);

  private final SchemaObject schemaObject;
  private final SimpleStatement statement;
  
  public DefaultDriverExceptionHandler(SchemaT schemaObject, SimpleStatement statement){

    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject must not be null");
    this.statement = Objects.requireNonNull(statement, "statement must not be null");
  }

  // Lower priority is more important, used when examining a list of errors from nodes
  private static int getExceptionPriority(Throwable exception) {
    return switch (exception) {
      case QueryConsistencyException e -> 0;
      case QueryExecutionException e -> 25;
      case QueryValidationException e -> 50;
      default -> Integer.MAX_VALUE;
    };
  }

  private static Optional<Throwable> findHighestPriority(AllNodesFailedException exception){
    var allExceptions = exception.getAllErrors().values().stream()
        .flatMap(List::stream)
        .toList();
    var highest =  findHighestPriority(allExceptions);

    if (LOGGER.isErrorEnabled()){
      LOGGER.error("AllNodesFailedException handled, highest priority exception was: {} original message: {}", highest, exception.getMessage());
    }
    return highest;
  }

  private static Optional<Throwable> findHighestPriority(List<Throwable> exceptions) {

    var sortedExceptions = new ArrayList<>(exceptions);
    sortedExceptions.sort(Comparator.comparingInt(DefaultDriverExceptionHandler::getExceptionPriority));
    return sortedExceptions.stream().findFirst();
  }

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  // Following exceptions fall back to the {@link #handleUnhandled(DriverException)}:
  //
  // - **CodecNotFoundException**
  //    - Happens if we try to encode/decode a type that does not have a codec in the driver.
  //    - The codecs we use to go to/from JSON should prevent this from happening.
  //
  // - **DriverExecutionException**
  //    - A catchall from the driver, should not happen.
  //
  // - **DriverTimeoutException**
  //    - Timeout on prepare and other non-user commands, should not happen.
  //
  // - **NodeUnavailableException**
  //    - Occurs when the driver configuration for max connections or in-flight requests is hit.
  //    - Nothing the user can do to change this, and it is not expected to happen.
  //
  // - **RequestThrottlingException**
  //    - Used for driver-level request throttling, which we are not using.
  //
  // - **UnsupportedProtocolVersionException**
  //    - Should not happen, as we control the protocol version.


  @Override
  public RuntimeException handle(ClosedConnectionException exception) {
    return DatabaseException.Code.CLOSED_CONNECTION.get(errVars(exception));
  }

  @Override
  public RuntimeException handle(DriverExecutionException exception) {
    // see the docs, this is often a wrapper for checked exceptions so re-handle if this is the case
    // otherwise it is unexpected
    return (exception.getCause() instanceof DriverException de)
        ? handle(de)
        : handleUnhandled(exception);
  }

  @Override
  public RuntimeException handle(InvalidKeyspaceException exception) {
    return DatabaseException.Code.UNKNOWN_KEYSPACE.get(errVars(exception));
  }

  // ========================================================================
  // AllNodesFailedException and subclasses
  // ========================================================================

  @Override
  public RuntimeException handle(AllNodesFailedException exception) {
    // Should always be created with errors from calling each node, re-process the most important error

    var highestPriority = findHighestPriority(exception)
        .orElseGet(() -> null);

    return switch (highestPriority) {
      // found a node specific error that is a driver based
      case DriverException e -> handle(e);
      // this is a non-driver based exception, so map to generic unexpected driver error
      case RuntimeException re -> DatabaseException.Code.UNEXPECTED_DRIVER_ERROR.get(errVars(re));
      // could not work out what the node error was OR this was a subclass of the AllNodesFailedException
      // this will be the null case, but also need a default label
      case null -> DriverExceptionHandler.super.handle(exception);
      default -> DriverExceptionHandler.super.handle(exception);
    };
  }

  @Override
  public RuntimeException handle(NoNodeAvailableException exception) {
    // this is a special case of AllNodesFailedException where no nodes were available
    return DatabaseException.Code.UNAVAILABLE_DATABASE.get(errVars(exception));
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  // Following exceptions fall back to the {@link #handleUnhandled(DriverException)}:
  //
  // - **AlreadyExistsException**
  //    - This should be handled by the subclasses of this handler because they know the specific entity we are trying to create.
  //    - See {@link CreateTableDriverExceptionHandler} for an example.
  //
  // - **InvalidConfigurationInQueryException**
  //    - Happens if we get the DDL command wrong.
  //    - Should not happen.

  @Override
  public RuntimeException handle(InvalidQueryException exception) {
    // this is the InvalidRequestException in the Cassandra code base, there are 300+ usages in it.
    // generally means while the syntax is correct, what you are asking it to do is not possible
    // e.g. add to a list, but the column is not a list.
    // the Data API should prevent this from happening, but if it does, it is a bug and maybe the user can fix it.



  }

  @Override
  public RuntimeException handle(UnauthorizedException exception) {
    return DatabaseException.Code.UNAUTHORIZED_ACCESS.get(errVars(exception));
  }


  // ========================================================================
  // QueryConsistencyException and subclasses
  // see commend for the QueryExecutionException section
  // ========================================================================

  @Override
  public RuntimeException handle(WriteTimeoutException exception) {
    return DatabaseException.Code.TABLE_WRITE_TIMEOUT.get(
        errVars(
            exception,
            m -> {
              m.put("blockFor", String.valueOf(exception.getBlockFor()));
              m.put("received", String.valueOf(exception.getReceived()));
            }));
  }
}
