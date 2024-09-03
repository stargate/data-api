package io.stargate.sgv2.jsonapi.exception.mappers;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.*;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple mapper for mapping {@link Throwable}s to {@link CommandResult.Error}, with a default
 * implementation.
 */
public final class ThrowableToErrorMapper {
  private static final Logger logger = LoggerFactory.getLogger(ThrowableToErrorMapper.class);

  private static final BiFunction<Throwable, String, CommandResult.Error> MAPPER_WITH_MESSAGE =
      (throwable, message) -> {
        // if our own exception, shortcut
        if (throwable instanceof JsonApiException jae) {
          if (jae.getErrorCode().equals(ErrorCode.SERVER_EMBEDDING_GATEWAY_NOT_AVAILABLE)) {
            // 30-Aug-2021: [data-api#1383] Why is this special case here?
            return jae.getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
          }
          return jae.getCommandResultError(message, jae.getHttpStatus());
        }

        // General Exception related to JSON handling, thrown by Jackson
        if (throwable instanceof JacksonException jacksonE) {
          return handleJsonProcessingException(jacksonE, message);
        }

        // UnauthorizedException from quarkus
        if (throwable instanceof UnauthorizedException) {
          return ErrorCode.UNAUTHENTICATED_REQUEST
              .toApiException()
              .getCommandResultError(message, Response.Status.UNAUTHORIZED);
        }

        // TimeoutException from quarkus
        if (throwable instanceof TimeoutException) {
          return ErrorCode.EMBEDDING_PROVIDER_TIMEOUT
              .toApiException()
              .getCommandResultError(Response.Status.OK);
        }

        // handle all driver exceptions
        if (throwable instanceof DriverException) {
          return handleDriverException((DriverException) throwable, message);
        }

        // handle an invalid Content-Type header
        if (throwable instanceof NotSupportedException) {
          // validate the Content-Type header, 415 if failed
          return ErrorCode.INVALID_CONTENT_TYPE_HEADER
              .toApiException()
              .getCommandResultError(Response.Status.UNSUPPORTED_MEDIA_TYPE);
        }

        // handle all other exceptions
        return handleUnrecognizedException(throwable, message);
      };

  private static CommandResult.Error handleDriverException(
      DriverException throwable, String message) {
    if (throwable instanceof AllNodesFailedException) {
      return handleAllNodesFailedException((AllNodesFailedException) throwable, message);
    } else if (throwable instanceof ClosedConnectionException) {
      return ErrorCode.SERVER_CLOSED_CONNECTION
          .toApiException("(DriverException/ClosedConnectionException) %s", message)
          .getCommandResultError(Response.Status.BAD_GATEWAY);
    } else if (throwable instanceof CoordinatorException) {
      return handleCoordinatorException((CoordinatorException) throwable, message);
    } else if (throwable instanceof DriverTimeoutException) {
      return ErrorCode.SERVER_DRIVER_TIMEOUT
          .toApiException("(DriverException/DriverTimeoutException) %s", message)
          .getCommandResultError(Response.Status.GATEWAY_TIMEOUT);
    } else {
      // Leave this as 500 since we do not recognize the exception: should add new cases
      // when we encounter new exceptions
      return ErrorCode.SERVER_DRIVER_FAILURE
          .toApiException(
              "(DriverException/unrecognized) root cause: (%s) %s",
              throwable.getClass().getName(), message)
          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private static CommandResult.Error handleCoordinatorException(
      CoordinatorException throwable, String message) {
    if (throwable instanceof QueryValidationException) {
      return handleQueryValidationException((QueryValidationException) throwable, message);
    } else if (throwable instanceof QueryExecutionException) {
      return handleQueryExecutionException((QueryExecutionException) throwable, message);
    } else {
      // Leave this as 500 since we do not recognize the exception: should add new cases
      // when we encounter new exceptions
      return ErrorCode.SERVER_COORDINATOR_FAILURE
          .toApiException(
              "(CoordinatorException/unrecognized) root cause: (%s) %s",
              throwable.getClass().getName(), message)
          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private static CommandResult.Error handleQueryExecutionException(
      QueryExecutionException throwable, String message) {
    if (throwable instanceof QueryConsistencyException e) {
      if (e instanceof WriteTimeoutException || e instanceof ReadTimeoutException) {
        return ErrorCode.SERVER_DRIVER_TIMEOUT
            .toApiException(
                "(QueryConsistencyException/%s) %s", e.getClass().getSimpleName(), message)
            .getCommandResultError(Response.Status.GATEWAY_TIMEOUT);
      } else if (e instanceof ReadFailureException) {
        return ErrorCode.SERVER_READ_FAILED
            .toApiException("(QueryConsistencyException/ReadFailureException) %s", message)
            .getCommandResultError(Response.Status.BAD_GATEWAY);
      } else {
        // Leave this as 500 since we do not recognize the exception: should add new cases
        // when we encounter new exceptions
        return ErrorCode.SERVER_QUERY_CONSISTENCY_FAILURE
            .toApiException(
                "(QueryConsistencyException/unrecognized) root cause: (%s) %s",
                throwable.getClass().getName(), message)
            .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
      }
    } else {
      // Leave this as 500 since we do not recognize the exception: should add new cases
      // when we encounter new exceptions
      return ErrorCode.SERVER_QUERY_EXECUTION_FAILURE
          .toApiException(
              "(QueryExecutionException/unrecognized) root cause: (%s) %s",
              throwable.getClass().getName(), message)
          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private static CommandResult.Error handleQueryValidationException(
      QueryValidationException throwable, String message) {
    if (throwable instanceof com.datastax.oss.driver.api.core.servererrors.UnauthorizedException) {
      return ErrorCode.UNAUTHENTICATED_REQUEST
          .toApiException()
          .getCommandResultError(
              ErrorCode.UNAUTHENTICATED_REQUEST.getMessage(), Response.Status.UNAUTHORIZED);
    } else if (message.contains(
            "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING")
        || message.contains("ANN ordering by vector requires the column to be indexed")) {
      return ErrorCode.NO_INDEX_ERROR
          .toApiException()
          .getCommandResultError(ErrorCode.NO_INDEX_ERROR.getMessage(), Response.Status.OK);
    }
    if (message.contains("vector<float,")) {
      // It is tricky to find the actual vector dimension from the message, include as-is
      return ErrorCode.VECTOR_SIZE_MISMATCH
          .toApiException("root cause = (%s) %s", throwable.getClass().getSimpleName(), message)
          .getCommandResultError(Response.Status.OK);
    }
    return ErrorCode.INVALID_QUERY
        .toApiException()
        .getCommandResultError(message, Response.Status.OK);
  }

  /** Driver AllNodesFailedException a composite exception, peeling the errors from it */
  private static CommandResult.Error handleAllNodesFailedException(
      AllNodesFailedException throwable, String message) {
    Map<Node, List<Throwable>> nodewiseErrors = throwable.getAllErrors();
    if (!nodewiseErrors.isEmpty()) {
      List<Throwable> errors = nodewiseErrors.values().iterator().next();
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
          return ErrorCode.UNAUTHENTICATED_REQUEST
              .toApiException()
              .getCommandResultError(
                  ErrorCode.UNAUTHENTICATED_REQUEST.getMessage(), Response.Status.UNAUTHORIZED);
          // Driver NoNodeAvailableException -> ErrorCode.NO_NODE_AVAILABLE
        } else if (error instanceof NoNodeAvailableException) {
          return ErrorCode.SERVER_NO_NODE_AVAILABLE
              .toApiException("(AllNodesFailedException/NoNodeAvailableException) %s", message)
              .getCommandResultError(message, Response.Status.BAD_GATEWAY);
        } else if (error instanceof DriverTimeoutException) {
          // [data-api#1205] Need to map DriverTimeoutException as well
          return ErrorCode.SERVER_DRIVER_TIMEOUT
              .toApiException(
                  Response.Status.GATEWAY_TIMEOUT,
                  "(AllNodesFailedException/DriverTimeoutException) %s",
                  message)
              .getCommandResultError();
        }
      }
    }

    // should not happen
    return handleUnrecognizedException(throwable, message);
  }

  private static CommandResult.Error handleJsonProcessingException(
      JacksonException e, String message) {
    if (e instanceof JsonParseException) {
      // Low-level parsing problem? Actual BAD_REQUEST (400) since we could not process
      return ErrorCode.INVALID_REQUEST_NOT_JSON
          .toApiException(
              Response.Status.BAD_REQUEST,
              "underlying problem: (%s) %s",
              e.getClass().getName(),
              message)
          .getCommandResultError();
    }
    // Unrecognized property? (note: CommandObjectMapperHandler handles some cases)
    if (e instanceof UnrecognizedPropertyException upe) {
      final Collection<Object> knownIds =
          Optional.ofNullable(upe.getKnownPropertyIds()).orElse(Collections.emptyList());
      final String knownDesc =
          knownIds.stream()
              .map(ob -> String.format("\"%s\"", ob.toString()))
              .sorted()
              .collect(Collectors.joining(", "));
      return ErrorCode.INVALID_REQUEST_UNKNOWN_FIELD
          .toApiException(
              "\"%s\" not one of known fields (%s) at '%s'",
              upe.getPropertyName(), knownDesc, upe.getPathReference())
          .getCommandResultError();
    }

    // Will need to add more handling but start with above
    return handleUnrecognizedException(e, message);
  }

  private static CommandResult.Error handleUnrecognizedException(
      Throwable throwable, String message) {
    logger.error(
        String.format(
            "Unrecognized Exception (%s) caught, mapped to SERVER_UNHANDLED_ERROR: %s",
            throwable.getClass().getName(), message),
        throwable);
    return ErrorCode.SERVER_UNHANDLED_ERROR
        .toApiException("root cause: (%s) %s", throwable.getClass().getName(), message)
        .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
  }

  private static final Function<Throwable, CommandResult.Error> MAPPER =
      throwable -> {
        String message = throwable.getMessage();
        if (message == null) {
          message = "Unexpected exception occurred.";
        }
        return MAPPER_WITH_MESSAGE.apply(throwable, message);
      };

  private ThrowableToErrorMapper() {}

  public static Function<Throwable, CommandResult.Error> getMapperFunction() {
    return MAPPER;
  }

  public static BiFunction<Throwable, String, CommandResult.Error> getMapperWithMessageFunction() {
    return MAPPER_WITH_MESSAGE;
  }
}
