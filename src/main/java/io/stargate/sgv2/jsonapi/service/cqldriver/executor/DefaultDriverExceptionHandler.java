package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants.TemplateVars;
import io.stargate.sgv2.jsonapi.exception.AuthException;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import java.util.List;
import java.util.Map;

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
    implements DriverExceptionHandler<SchemaT> {

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  @Override
  public RuntimeException handle(SchemaT schemaObject, ClosedConnectionException exception) {
    return DatabaseException.Code.CLOSED_CONNECTION.get(
        errVars(schemaObject, map -> map.put(TemplateVars.ERROR_MESSAGE, exception.getMessage())));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, DriverTimeoutException exception) {
    return DatabaseException.Code.DRIVER_TIMEOUT.get(
        errVars(schemaObject, map -> map.put(TemplateVars.ERROR_MESSAGE, exception.getMessage())));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, AllNodesFailedException exception) {
    Map<Node, List<Throwable>> allErrors = exception.getAllErrors();
    if (!allErrors.isEmpty()) {
      List<Throwable> errors = allErrors.values().iterator().next();
      if (errors != null && !errors.isEmpty()) {
        Throwable error =
            errors.stream()
                .findAny()
                .filter(
                    t ->
                        t instanceof AuthenticationException
                            || t instanceof IllegalArgumentException
                            || t instanceof NoNodeAvailableException
                            || t instanceof DriverTimeoutException)
                .orElse(null);
        // connect to oss cassandra throws AuthenticationException for invalid credentials
        // connect to AstraDB throws IllegalArgumentException for invalid token/credentials
        if (error instanceof AuthenticationException
            || (error instanceof IllegalArgumentException
                && (error.getMessage().contains("AUTHENTICATION ERROR")
                    || error
                        .getMessage()
                        .contains("Provided username token and/or password are incorrect")))) {
          // TODO(Hazel): AuthException and INVALID_TOKEN?
          return AuthException.Code.INVALID_TOKEN.get(errVars(exception));
          // Driver NoNodeAvailableException -> ErrorCode.NO_NODE_AVAILABLE
        } else if (error instanceof NoNodeAvailableException) {
          return handle(schemaObject, (NoNodeAvailableException) error);
        } else if (error instanceof DriverTimeoutException) {
          // [data-api#1205] Need to map DriverTimeoutException as well
          return handle(schemaObject, (DriverTimeoutException) error);
        }
      }
    }
    return exception;
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, NoNodeAvailableException exception) {
    return DatabaseException.Code.NO_NODE_AVAILABLE.get(
        errVars(schemaObject, map -> map.put(TemplateVars.ERROR_MESSAGE, exception.getMessage())));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, QueryValidationException exception) {
    String message = exception.getMessage();
    if (message.contains(
            "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING")
        || message.contains("ANN ordering by vector requires the column to be indexed")) {
      // TODO(Hazel): Original code is NO_INDEX_ERROR, I think we need to change but am not sure
      // what to change
      return exception;
    }
    if (message.contains("vector<float,")) {
      // It is tricky to find the actual vector dimension from the message, include as-is
      // TODO(Hazel): the code VECTOR_SIZE_MISMATCH was added recently, should we keep using it?
      return exception;
    }
    // Reuse the default method in the interface
    return DriverExceptionHandler.super.handle(schemaObject, exception);
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, UnauthorizedException exception) {
    return AuthException.Code.INVALID_TOKEN.get(errVars(exception));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, ReadFailureException exception) {
    return DatabaseException.Code.READ_FAILURE.get(
        errVars(
            schemaObject,
            m -> {
              m.put("blockFor", String.valueOf(exception.getBlockFor()));
              m.put("received", String.valueOf(exception.getReceived()));
              m.put("numFailures", String.valueOf(exception.getNumFailures()));
            }));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, ReadTimeoutException exception) {
    return DatabaseException.Code.READ_TIMEOUT.get(
        errVars(
            schemaObject,
            m -> {
              m.put("blockFor", String.valueOf(exception.getBlockFor()));
              m.put("received", String.valueOf(exception.getReceived()));
            }));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, WriteFailureException exception) {
    return DatabaseException.Code.WRITE_FAILURE.get(
        errVars(
            schemaObject,
            m -> {
              m.put("blockFor", String.valueOf(exception.getBlockFor()));
              m.put("received", String.valueOf(exception.getReceived()));
              m.put("numFailures", String.valueOf(exception.getNumFailures()));
            }));
  }

  @Override
  public RuntimeException handle(SchemaT schemaObject, WriteTimeoutException exception) {
    return DatabaseException.Code.WRITE_TIMEOUT.get(
        errVars(
            schemaObject,
            m -> {
              m.put("blockFor", String.valueOf(exception.getBlockFor()));
              m.put("received", String.valueOf(exception.getReceived()));
            }));
  }
}
