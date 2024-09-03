package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.playing.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import io.stargate.sgv2.jsonapi.exception.playing.ServerException;

/**
 * Interface for handling exceptions from the Java driver.
 *
 * <p>The interface encapsulates all the different driver errors (there are a lot) and provides a
 * type specific <code>handle()</code> functions for all the ones we could possibly care about. Kept
 * as an interface so any implementations do not need to worry about all the types and casting.
 *
 * <p>Users should create an instance of the {@link DefaultDriverExceptionHandler} or an appropriate
 * subclass and call {@link #maybeHandle(SchemaObject, Throwable)} when they get an error from the
 * driver, then throw the result or otherwise handle it.
 *
 * <p>Implementations should override the handle() functions for the errors they care about. The
 * default is for the <code>handle()</code> function to return the object unchanged. If an exception
 * is not changed to a different object then {@link #maybeHandle(SchemaObject, Throwable)} will call
 * {@link #handleUnhandled(SchemaObject, DriverException)} as a last chance to change the driver
 * exception into something else.
 *
 * <p><b>NOTE:</b> Subclass {@link DefaultDriverExceptionHandler} rather than implement this
 * interface directly.
 *
 * @param <T> The type of the {@link SchemaObject} that the CQL command was operating against.
 */
public interface DriverExceptionHandler<T extends SchemaObject> {

  /**
   * If the <code>throwable</code> is a {@link DriverException} (and non-null), then calls the
   * appropriately typed handle() function.
   *
   * <p>If the handle() function returns the same object as was passed in, then the exception is
   * passed to {@link #handleUnhandled(SchemaObject, DriverException)}.
   *
   * @param schemaObject Schema object the CQL command was operating against
   * @param throwable The exception to handle
   * @return The exception to throw or otherwise handle, may be the original exception or a new one.
   */
  default Throwable maybeHandle(T schemaObject, Throwable throwable) {
    if (throwable instanceof DriverException) {
      var handled = handle(schemaObject, (DriverException) throwable);
      return handled == throwable
          ? handleUnhandled(schemaObject, (DriverException) throwable)
          : handled;
    }
    return throwable;
  }

  /**
   * Called when an exception was not changed by any handler functions, default is to return a
   * {@link ServerException.Code#UNEXPECTED_SERVER_ERROR} with the error message from the exception.
   *
   * @param schemaObject The schema object the CQL command was operating against.
   * @param exception The exception that was not handled.
   * @return Default {@link ServerException.Code#UNEXPECTED_SERVER_ERROR}
   */
  default Throwable handleUnhandled(T schemaObject, DriverException exception) {
    return ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errFmt(exception));
  }

  default RuntimeException handle(T schemaObject, DriverException exception) {
    return switch (exception) {
        // checking the subclasses that have children first, the handlers for these should
        // cast for their children
      case AllNodesFailedException e -> handle(schemaObject, e);
      case QueryValidationException e -> handle(schemaObject, e);
      case QueryExecutionException e -> handle(schemaObject, e);
        // all these are direct subclasses of DriverException with no children
      case ClosedConnectionException e -> handle(schemaObject, e);
      case CodecNotFoundException e -> handle(schemaObject, e);
      case DriverExecutionException e -> handle(schemaObject, e);
      case DriverTimeoutException e -> handle(schemaObject, e);
      case InvalidKeyspaceException e -> handle(schemaObject, e);
      case NodeUnavailableException e -> handle(schemaObject, e);
      case RequestThrottlingException e -> handle(schemaObject, e);
      case UnsupportedProtocolVersionException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  default RuntimeException handle(T schemaObject, ClosedConnectionException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, CodecNotFoundException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, DriverExecutionException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, DriverTimeoutException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, InvalidKeyspaceException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, NodeUnavailableException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, RequestThrottlingException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, UnsupportedProtocolVersionException exception) {
    return exception;
  }

  // ========================================================================
  // AllNodesFailedException and subclasses
  // ========================================================================

  default RuntimeException handle(T schemaObject, AllNodesFailedException exception) {
    return switch (exception) {
      case NoNodeAvailableException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(T schemaObject, NoNodeAvailableException exception) {
    return exception;
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  default RuntimeException handle(T schemaObject, QueryValidationException exception) {
    return switch (exception) {
      case AlreadyExistsException e -> handle(schemaObject, e);
      case InvalidConfigurationInQueryException e -> handle(schemaObject, e);
      case InvalidQueryException e -> handle(schemaObject, e);
      case SyntaxError e -> handle(schemaObject, e);
      case UnauthorizedException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(T schemaObject, AlreadyExistsException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, InvalidConfigurationInQueryException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, InvalidQueryException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, SyntaxError exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, UnauthorizedException exception) {
    return exception;
  }

  // ========================================================================
  // QueryExecutionException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // - QueryConsistencyException is a sublcass of QueryExecutionException,
  // it is in it's own section so the re-casting in switch allows handling both
  // QueryConsistencyException and it's childs (cannot pattern match on parent and child)
  // ========================================================================

  default RuntimeException handle(T schemaObject, QueryExecutionException exception) {
    return switch (exception) {
      case BootstrappingException e -> handle(schemaObject, e);
      case CASWriteUnknownException e -> handle(schemaObject, e);
      case CDCWriteFailureException e -> handle(schemaObject, e);
      case FunctionFailureException e -> handle(schemaObject, e);
      case OverloadedException e -> handle(schemaObject, e);
      case QueryConsistencyException e -> handle(schemaObject, e);
      case TruncateException e -> handle(schemaObject, e);
      case UnavailableException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(T schemaObject, BootstrappingException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, CASWriteUnknownException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, CDCWriteFailureException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, FunctionFailureException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, OverloadedException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, TruncateException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, UnavailableException exception) {
    return exception;
  }

  // ========================================================================
  // QueryConsistencyException and subclasses
  // see commend for the QueryExecutionException section
  // ========================================================================

  default RuntimeException handle(T schemaObject, QueryConsistencyException exception) {
    return switch (exception) {
      case ReadFailureException e -> handle(schemaObject, e);
      case ReadTimeoutException e -> handle(schemaObject, e);
      case WriteFailureException e -> handle(schemaObject, e);
      case WriteTimeoutException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(T schemaObject, ReadFailureException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, ReadTimeoutException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, WriteFailureException exception) {
    return exception;
  }

  default RuntimeException handle(T schemaObject, WriteTimeoutException exception) {
    return exception;
  }
}
