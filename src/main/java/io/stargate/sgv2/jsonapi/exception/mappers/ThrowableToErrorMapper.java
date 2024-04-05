package io.stargate.sgv2.jsonapi.exception.mappers;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.*;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Simple mapper for mapping {@link Throwable}s to {@link CommandResult.Error}, with a default
 * implementation.
 */
public final class ThrowableToErrorMapper {
  private static final BiFunction<Throwable, String, CommandResult.Error> MAPPER_WITH_MESSAGE =
      (throwable, message) -> {
        // if our own exception, shortcut
        if (throwable instanceof JsonApiException jae) {
          return jae.getCommandResultError(message, Response.Status.OK);
        }

        // UnauthorizedException from quarkus
        if (throwable instanceof UnauthorizedException) {
          return ErrorCode.UNAUTHENTICATED_REQUEST
              .toApiException()
              .getCommandResultError(
                  ErrorCode.UNAUTHENTICATED_REQUEST.getMessage(), Response.Status.UNAUTHORIZED);
        }

        // handle all driver exceptions
        if (throwable instanceof DriverException) {
          return handleDriverException((DriverException) throwable, message);
        }

        // handle all other exceptions
        return ErrorCode.SERVER_UNHANDLED_ERROR
            .toApiException(
                "root cause: (%s) %s", throwable.getClass().getName(), throwable.getMessage())
            .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
      };

  private static CommandResult.Error handleDriverException(
      DriverException throwable, String message) {
    if (throwable instanceof AllNodesFailedException) {
      return handleAllNodesFailedException((AllNodesFailedException) throwable, message);
    } else if (throwable instanceof ClosedConnectionException) {
      return ErrorCode.SERVER_CLOSED_CONNECTION
          .toApiException()
          .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
    } else if (throwable instanceof CoordinatorException) {
      return handleCoordinatorException((CoordinatorException) throwable, message);
    } else if (throwable instanceof DriverTimeoutException) {
      return ErrorCode.SERVER_TIMEOUT
          .toApiException()
          .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
    } else {
      return ErrorCode.SERVER_FAILURE
          .toApiException(
              "root cause: (%s) %s", throwable.getClass().getName(), throwable.getMessage())
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
      return ErrorCode.SERVER_COORDINATOR_FAILURE
          .toApiException(
              "root cause: (%s) %s", throwable.getClass().getName(), throwable.getMessage())
          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private static CommandResult.Error handleQueryExecutionException(
      QueryExecutionException throwable, String message) {
    if (throwable instanceof QueryConsistencyException e) {
      if (e instanceof WriteTimeoutException || e instanceof ReadTimeoutException) {
        return ErrorCode.SERVER_TIMEOUT
            .toApiException()
            .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
      } else if (e instanceof ReadFailureException) {
        return ErrorCode.SERVER_READ_FAILED
            .toApiException("root cause: (%s) %s", e.getClass().getName(), e.getMessage())
            .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
      } else {
        return ErrorCode.SERVER_QUERY_CONSISTENCY_FAILURE
            .toApiException("root cause: (%s) %s", e.getClass().getName(), e.getMessage())
            .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
      }
    } else {
      return ErrorCode.SERVER_QUERY_EXECUTION_FAILURE
          .toApiException(
              "root cause: (%s) %s", throwable.getClass().getName(), throwable.getMessage())
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
    String errorMessage =
        message.contains("vector<float,") ? "Mismatched vector dimension" : message;
    return ErrorCode.INVALID_QUERY
        .toApiException()
        .getCommandResultError(errorMessage, Response.Status.OK);
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
                            || t instanceof NoNodeAvailableException)
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
              .toApiException()
              .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
        }
      }
    }
    // should not happen
    return ErrorCode.SERVER_UNHANDLED_ERROR
        .toApiException(
            "root cause: (%s) %s", throwable.getClass().getName(), throwable.getMessage())
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
