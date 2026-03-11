package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import io.stargate.sgv2.jsonapi.exception.ExceptionHandler;

/**
 * Interface for handling exceptions from the Java driver.
 *
 * <p>See docs for {@link ExceptionHandler} for more details.
 *
 * <p>The interface encapsulates all the different driver errors (there are a lot) and provides a
 * type specific <code>handle()</code> functions for all the ones we could possibly care about. Kept
 * as an interface so any implementations do not need to worry about all the types and casting.
 *
 * <p>Users should create an instance of the {@link DefaultDriverExceptionHandler} or an appropriate
 * subclass.
 *
 * <p><b>NOTE:</b> Subclass {@link DefaultDriverExceptionHandler} rather than implement this
 * interface directly.
 */
public interface DriverExceptionHandler extends ExceptionHandler<DriverException> {

  @Override
  default Class<DriverException> getExceptionClass() {
    return DriverException.class;
  }

  @Override
  default Throwable handle(DriverException exception) {
    return switch (exception) {
        // checking the subclasses that have children first, the handlers for these should
        // cast for their children
      case AllNodesFailedException e -> handle(e);
      case APIDriverException e -> handle(e);
      case QueryValidationException e -> handle(e);
      case QueryExecutionException e -> handle(e);
        // all these are direct subclasses of DriverException with no children
      case ClosedConnectionException e -> handle(e);
      case CodecNotFoundException e -> handle(e);
      case DriverExecutionException e -> handle(e);
      case DriverTimeoutException e -> handle(e);
      case InvalidKeyspaceException e -> handle(e);
      case NodeUnavailableException e -> handle(e);
      case RequestThrottlingException e -> handle(e);
      case UnsupportedProtocolVersionException e -> handle(e);
      default -> exception;
    };
  }

  // ========================================================================
  // Special case - Driver Exceptions that are not subclasses of DriverException
  // which have been remapped to a APIDriverException subclass
  // ========================================================================

  default Throwable handle(APIDriverException exception) {
    return switch (exception) {
      case AuthenticationDriverException e -> handle(e);
      default -> exception;
    };
  }

  default Throwable handle(AuthenticationDriverException exception) {
    return exception;
  }

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  default Throwable handle(ClosedConnectionException exception) {
    return exception;
  }

  default Throwable handle(CodecNotFoundException exception) {
    return exception;
  }

  default Throwable handle(DriverExecutionException exception) {
    return exception;
  }

  default Throwable handle(DriverTimeoutException exception) {
    return exception;
  }

  default Throwable handle(InvalidKeyspaceException exception) {
    return exception;
  }

  default Throwable handle(NodeUnavailableException exception) {
    return exception;
  }

  default Throwable handle(RequestThrottlingException exception) {
    return exception;
  }

  default Throwable handle(UnsupportedProtocolVersionException exception) {
    return exception;
  }

  // ========================================================================
  // AllNodesFailedException and subclasses
  // ========================================================================

  default Throwable handle(AllNodesFailedException exception) {
    return switch (exception) {
      case NoNodeAvailableException e -> handle(e);
      default -> exception;
    };
  }

  default Throwable handle(NoNodeAvailableException exception) {
    return exception;
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  default Throwable handle(QueryValidationException exception) {
    return switch (exception) {
      case AlreadyExistsException e -> handle(e);
      case InvalidConfigurationInQueryException e -> handle(e);
      case InvalidQueryException e -> handle(e);
      case SyntaxError e -> handle(e);
      case UnauthorizedException e -> handle(e);
      default -> exception;
    };
  }

  default Throwable handle(AlreadyExistsException exception) {
    return exception;
  }

  default Throwable handle(InvalidConfigurationInQueryException exception) {
    return exception;
  }

  default Throwable handle(InvalidQueryException exception) {
    return exception;
  }

  default Throwable handle(SyntaxError exception) {
    return exception;
  }

  default Throwable handle(UnauthorizedException exception) {
    return exception;
  }

  // ========================================================================
  // QueryExecutionException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // - QueryConsistencyException is a sublcass of QueryExecutionException,
  // it is in its own section so the re-casting in switch allows handling both
  // QueryConsistencyException and its childs (cannot pattern match on parent and child)
  // ========================================================================

  default Throwable handle(QueryExecutionException exception) {
    return switch (exception) {
      case BootstrappingException e -> handle(e);
      case CASWriteUnknownException e -> handle(e);
      case CDCWriteFailureException e -> handle(e);
      case FunctionFailureException e -> handle(e);
      case OverloadedException e -> handle(e);
      case QueryConsistencyException e -> handle(e);
      case TruncateException e -> handle(e);
      case UnavailableException e -> handle(e);
      default -> exception;
    };
  }

  default Throwable handle(BootstrappingException exception) {
    return exception;
  }

  default Throwable handle(CASWriteUnknownException exception) {
    return exception;
  }

  default Throwable handle(CDCWriteFailureException exception) {
    return exception;
  }

  default Throwable handle(FunctionFailureException exception) {
    return exception;
  }

  default Throwable handle(OverloadedException exception) {
    return exception;
  }

  default Throwable handle(TruncateException exception) {
    return exception;
  }

  default Throwable handle(UnavailableException exception) {
    return exception;
  }

  // ========================================================================
  // QueryConsistencyException and subclasses
  // see commend for the QueryExecutionException section
  // ========================================================================

  default Throwable handle(QueryConsistencyException exception) {
    return switch (exception) {
      case ReadFailureException e -> handle(e);
      case ReadTimeoutException e -> handle(e);
      case WriteFailureException e -> handle(e);
      case WriteTimeoutException e -> handle(e);
      default -> exception;
    };
  }

  default Throwable handle(ReadFailureException exception) {
    return exception;
  }

  default Throwable handle(ReadTimeoutException exception) {
    return exception;
  }

  default Throwable handle(WriteFailureException exception) {
    return exception;
  }

  default Throwable handle(WriteTimeoutException exception) {
    return exception;
  }
}
