package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.*;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import io.stargate.sgv2.jsonapi.exception.playing.ExceptionHandler;

/**
 * Interface for handling exceptions from the Java driver.
 *
 * <p>See docs for {@link io.stargate.sgv2.jsonapi.exception.playing.ExceptionHandler} for more
 * details.
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
 *
 * @param <SchemaT> The type of the {@link SchemaObject} that the CQL command was operating against.
 */
public interface DriverExceptionHandler<SchemaT extends SchemaObject>
    extends ExceptionHandler<SchemaT, DriverException> {

  @Override
  default Class<DriverException> getExceptionClass() {
    return DriverException.class;
  }

  default RuntimeException handle(SchemaT schemaObject, DriverException exception) {
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

  default RuntimeException handle(SchemaT schemaObject, ClosedConnectionException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, CodecNotFoundException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, DriverExecutionException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, DriverTimeoutException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, InvalidKeyspaceException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, NodeUnavailableException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, RequestThrottlingException exception) {
    return exception;
  }

  default RuntimeException handle(
      SchemaT schemaObject, UnsupportedProtocolVersionException exception) {
    return exception;
  }

  // ========================================================================
  // AllNodesFailedException and subclasses
  // ========================================================================

  default RuntimeException handle(SchemaT schemaObject, AllNodesFailedException exception) {
    return switch (exception) {
      case NoNodeAvailableException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(SchemaT schemaObject, NoNodeAvailableException exception) {
    return exception;
  }

  // ========================================================================
  // QueryValidationException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // ========================================================================

  default RuntimeException handle(SchemaT schemaObject, QueryValidationException exception) {
    return switch (exception) {
      case AlreadyExistsException e -> handle(schemaObject, e);
      case InvalidConfigurationInQueryException e -> handle(schemaObject, e);
      case InvalidQueryException e -> handle(schemaObject, e);
      case SyntaxError e -> handle(schemaObject, e);
      case UnauthorizedException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(SchemaT schemaObject, AlreadyExistsException exception) {
    return exception;
  }

  default RuntimeException handle(
      SchemaT schemaObject, InvalidConfigurationInQueryException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, InvalidQueryException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, SyntaxError exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, UnauthorizedException exception) {
    return exception;
  }

  // ========================================================================
  // QueryExecutionException and subclasses
  // - this is a subclass CoordinatorException but that is abstract
  // - QueryConsistencyException is a sublcass of QueryExecutionException,
  // it is in its own section so the re-casting in switch allows handling both
  // QueryConsistencyException and its childs (cannot pattern match on parent and child)
  // ========================================================================

  default RuntimeException handle(SchemaT schemaObject, QueryExecutionException exception) {
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

  default RuntimeException handle(SchemaT schemaObject, BootstrappingException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, CASWriteUnknownException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, CDCWriteFailureException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, FunctionFailureException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, OverloadedException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, TruncateException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, UnavailableException exception) {
    return exception;
  }

  // ========================================================================
  // QueryConsistencyException and subclasses
  // see commend for the QueryExecutionException section
  // ========================================================================

  default RuntimeException handle(SchemaT schemaObject, QueryConsistencyException exception) {
    return switch (exception) {
      case ReadFailureException e -> handle(schemaObject, e);
      case ReadTimeoutException e -> handle(schemaObject, e);
      case WriteFailureException e -> handle(schemaObject, e);
      case WriteTimeoutException e -> handle(schemaObject, e);
      default -> exception;
    };
  }

  default RuntimeException handle(SchemaT schemaObject, ReadFailureException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, ReadTimeoutException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, WriteFailureException exception) {
    return exception;
  }

  default RuntimeException handle(SchemaT schemaObject, WriteTimeoutException exception) {
    return exception;
  }
}
