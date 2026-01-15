package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import jakarta.ws.rs.NotSupportedException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * amorton - 7 jan 2026 refactor NOTES: leaving this named ThrowableToErrorMapper because we know it
 * as that name.
 *
 * <p>Changed the role to ONLY be remaping Java exception to exception. Previously it would also
 * build the Command error (CommandResult.Error) that is returned to the client. Made the change to
 * try simplifying as I work to remove this class completly.
 */
public final class ThrowableToErrorMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThrowableToErrorMapper.class);

  private ThrowableToErrorMapper() {}

  public static Throwable mapThrowable(Throwable throwable) {
    return mapThrowable(throwable, "Unexpected exception occurred.");
  }

  public static Throwable mapThrowable(Throwable throwable, String message) {

    // amorton - 7 jan 2026 - this could be better as pattern matching, but leaving until we remove
    // the whole class.

    // V2 error, normally handled in the Task processing but can be in other places
    if (throwable instanceof APIException) {
      return throwable;
    }

    // if our own V1 exception, shortcut
    if (throwable instanceof JsonApiException) {
      return throwable;
      //      return jae.getCommandResultError(message, jae.getHttpStatus());
    }

    // General Exception related to JSON handling, thrown by Jackson
    if (throwable instanceof JacksonException jacksonE) {
      return handleJsonProcessingException(jacksonE, message);
    }

    // UnauthorizedException from quarkus
    // TODO: AARON - why would this happen ?
    if (throwable instanceof UnauthorizedException) {
      return APISecurityException.Code.UNAUTHENTICATED_REQUEST.get();
      //          .getCommandResultError(Response.Status.UNAUTHORIZED);
    }

    // TimeoutException from quarkus
    // TODO: AARON - why would this happen ?
    if (throwable instanceof TimeoutException) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException();
      //          .getCommandResultError(Response.Status.OK);
    }

    // handle all driver exceptions
    //    if (throwable instanceof DriverException) {
    //      return handleDriverException((DriverException) throwable, message);
    //    }

    // handle an invalid Content-Type header
    if (throwable instanceof NotSupportedException nse) {
      // validate the Content-Type header, 415 if failed
      return RequestException.Code.UNSUPPORTED_CONTENT_TYPE.get("message", nse.getMessage());
      //          .getCommandResultError(Response.Status.UNSUPPORTED_MEDIA_TYPE);
    }

    // handle all other exceptions
    return handleUnrecognizedException(throwable, message);
  }
  ;

  //  private static Throwable handleDriverException(DriverException throwable, String message) {
  //
  //    if (throwable instanceof AllNodesFailedException) {
  //      return handleAllNodesFailedException((AllNodesFailedException) throwable, message);
  //
  //      // XXX NOTE: changes the return to internal server error
  //    } else if (throwable instanceof ClosedConnectionException) {
  //      return ErrorCodeV1.SERVER_CLOSED_CONNECTION
  //          .toApiException("(DriverException/ClosedConnectionException) %s", message)
  //          .getCommandResultError(Response.Status.BAD_GATEWAY);
  //
  //    } else if (throwable instanceof CoordinatorException) {
  //      return handleCoordinatorException((CoordinatorException) throwable, message);
  //
  //      // XXX NOTE: changes the return 200
  //    } else if (throwable instanceof DriverTimeoutException) {
  //      return ErrorCodeV1.SERVER_DRIVER_TIMEOUT
  //          .toApiException("(DriverException/DriverTimeoutException) %s", message)
  //          .getCommandResultError(Response.Status.GATEWAY_TIMEOUT);
  //
  //    } else {
  //      // Leave this as 500 since we do not recognize the exception: should add new cases
  //      // when we encounter new exceptions
  //      return ErrorCodeV1.SERVER_DRIVER_FAILURE
  //          .toApiException(
  //              "(DriverException/unrecognized) root cause: (%s) %s",
  //              throwable.getClass().getName(), message)
  //          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
  //    }
  //  }

  //  private static CommandResult.Error handleCoordinatorException(
  //      CoordinatorException throwable, String message) {
  //    if (throwable instanceof QueryValidationException) {
  //      return handleQueryValidationException((QueryValidationException) throwable, message);
  //    } else if (throwable instanceof QueryExecutionException) {
  //      return handleQueryExecutionException((QueryExecutionException) throwable, message);
  //    } else {
  //      // Leave this as 500 since we do not recognize the exception: should add new cases
  //      // when we encounter new exceptions
  //      return ErrorCodeV1.SERVER_COORDINATOR_FAILURE
  //          .toApiException(
  //              "(CoordinatorException/unrecognized) root cause: (%s) %s",
  //              throwable.getClass().getName(), message)
  //          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
  //    }
  //  }

  // XXX TODO: REVIEW HTTP status changes
  //  private static APIException handleQueryExecutionException(QueryExecutionException throwable,
  // String message) {
  //
  //    if (throwable instanceof QueryConsistencyException e) {
  //      if (e instanceof WriteTimeoutException || e instanceof ReadTimeoutException) {
  //        return ErrorCodeV1.SERVER_DRIVER_TIMEOUT
  //            .toApiException(
  //                "(QueryConsistencyException/%s) %s", e.getClass().getSimpleName(), message)
  //            .getCommandResultError(Response.Status.GATEWAY_TIMEOUT);
  //      } else if (e instanceof ReadFailureException) {
  //        return ErrorCodeV1.SERVER_READ_FAILED
  //            .toApiException("(QueryConsistencyException/ReadFailureException) %s", message)
  //            .getCommandResultError(Response.Status.BAD_GATEWAY);
  //      } else {
  //        // Leave this as 500 since we do not recognize the exception: should add new cases
  //        // when we encounter new exceptions
  //        return ErrorCodeV1.SERVER_QUERY_CONSISTENCY_FAILURE
  //            .toApiException(
  //                "(QueryConsistencyException/unrecognized) root cause: (%s) %s",
  //                throwable.getClass().getName(), message)
  //            .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
  //      }
  //    } else {
  //      // Leave this as 500 since we do not recognize the exception: should add new cases
  //      // when we encounter new exceptions
  //      return ErrorCodeV1.SERVER_QUERY_EXECUTION_FAILURE
  //          .toApiException(
  //              "(QueryExecutionException/unrecognized) root cause: (%s) %s",
  //              throwable.getClass().getName(), message)
  //          .getCommandResultError(Response.Status.INTERNAL_SERVER_ERROR);
  //    }
  //  }

  //  private static APIException handleQueryValidationException(QueryValidationException throwable,
  // String message) {

  //    if (throwable instanceof
  // com.datastax.oss.driver.api.core.servererrors.UnauthorizedException) {
  //      return SecurityException.Code.UNAUTHORIZED_ACCESS
  //          .get();
  //    } else if (message.contains(
  //            "If you want to execute this query despite the performance unpredictability, use
  // ALLOW FILTERING")
  //        || message.contains("ANN ordering by vector requires the column to be indexed")) {
  //      return ErrorCodeV1.NO_INDEX_ERROR
  //          .toApiException()
  //          .getCommandResultError(ErrorCodeV1.NO_INDEX_ERROR.getMessage(), Response.Status.OK);
  //    }

  // XXX : AARON : this should not happen ? we should check before sending it ?
  //    if (message.contains("vector<float,")) {
  //      // It is tricky to find the actual vector dimension from the message, include as-is
  //      return ErrorCodeV1.VECTOR_SIZE_MISMATCH
  //          .toApiException("root cause = (%s) %s", throwable.getClass().getSimpleName(), message)
  //          .getCommandResultError(Response.Status.OK);
  //    }

  // [data-api#1900]: Need to convert Lexical-index creation failure to something more meaningful
  //    if (message.contains("Invalid analyzer config")) {
  //      return SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS
  //          .get("message", message)
  //          .getCommandResultError(Response.Status.OK);
  //    }
  // [data-api#2068]: Need to convert Lexical-value-too-big failure to something more meaningful
  //    if (message.contains(
  //        "analyzed size for column query_lexical_value exceeds the cumulative limit for index"))
  // {
  //      return ErrorCodeV1.LEXICAL_CONTENT_TOO_BIG
  //          .toApiException()
  //          .getCommandResultError(Response.Status.OK);
  //    }
  //    return ErrorCodeV1.INVALID_QUERY
  //        .toApiException()
  //        .getCommandResultError(message, Response.Status.OK);
  //  }

  /** Driver AllNodesFailedException a composite exception, peeling the errors from it */
  //  private static CommandResult.Error handleAllNodesFailedException(AllNodesFailedException
  // throwable, String message) {
  //
  //    Map<Node, List<Throwable>> nodewiseErrors = throwable.getAllErrors();
  //    if (!nodewiseErrors.isEmpty()) {
  //      List<Throwable> errors = nodewiseErrors.values().iterator().next();
  //      if (errors != null && !errors.isEmpty()) {
  //        Throwable error =
  //            errors.stream()
  //                .findAny()
  //                .filter(
  //                    t ->
  //                        t instanceof AuthenticationException
  //                            || t instanceof IllegalArgumentException
  //                            || t instanceof NoNodeAvailableException
  //                            || t instanceof DriverTimeoutException)
  //                .orElse(null);
  //        // connect to oss cassandra throws AuthenticationException for invalid credentials
  //        // connect to AstraDB throws IllegalArgumentException for invalid token/credentials
  //        if (error instanceof AuthenticationException
  //            || (error instanceof IllegalArgumentException
  //                && (error.getMessage().contains("AUTHENTICATION ERROR")
  //                    || error
  //                        .getMessage()
  //                        .contains("Provided username token and/or password are incorrect")))) {
  //          return RequestException.Code.UNAUTHENTICATED_REQUEST
  //              .get()
  //              .getCommandResultError(Response.Status.UNAUTHORIZED);
  //          // Driver NoNodeAvailableException -> ErrorCode.NO_NODE_AVAILABLE
  //        } else if (error instanceof NoNodeAvailableException) {
  //          return ErrorCodeV1.SERVER_NO_NODE_AVAILABLE
  //              .toApiException("(AllNodesFailedException/NoNodeAvailableException) %s", message)
  //              .getCommandResultError(message, Response.Status.BAD_GATEWAY);
  //        } else if (error instanceof DriverTimeoutException) {
  //          // [data-api#1205] Need to map DriverTimeoutException as well
  //          return ErrorCodeV1.SERVER_DRIVER_TIMEOUT
  //              .toApiException(
  //                  Response.Status.GATEWAY_TIMEOUT,
  //                  "(AllNodesFailedException/DriverTimeoutException) %s",
  //                  message)
  //              .getCommandResultError();
  //        }
  //      }
  //    }
  //
  //    // should not happen
  //    return handleUnrecognizedException(throwable, message);
  //  }

  private static APIException handleJsonProcessingException(JacksonException e, String message) {

    if (e instanceof JsonParseException) {
      // Low-level parsing problem? Actual BAD_REQUEST (400) since we could not process
      return RequestException.Code.REQUEST_NOT_JSON.get(Map.of("errorMessage", e.getMessage()));
      //          .getCommandResultError(Response.Status.BAD_REQUEST);
    }

    // Unrecognized property? (note: CommandObjectMapperHandler handles some cases)
    if (e instanceof UnrecognizedPropertyException upe) {

      // 09-Oct-2025, tatu: Retain custom exception message, if set by us:
      if (ColumnDesc.class.equals(upe.getReferringClass())) {
        return RequestException.Code.COMMAND_FIELD_UNKNOWN.get(
            Map.of("field", upe.getPropertyName(), "message", upe.getOriginalMessage()));
        //            .getCommandResultError();
      }

      // otherwise rewrite to avoid Jackson-isms:
      final Collection<Object> knownIds =
          Optional.ofNullable(upe.getKnownPropertyIds()).orElse(Collections.emptyList());
      final String knownDesc =
          knownIds.stream()
              .map(ob -> String.format("'%s'", ob.toString()))
              .sorted()
              .collect(Collectors.joining(", "));
      return RequestException.Code.COMMAND_FIELD_UNKNOWN.get(
          Map.of(
              "field",
              upe.getPropertyName(),
              "message",
              "not one of known fields (%s)".formatted(knownDesc)));
      //          .getCommandResultError();
    }

    // aaron 28-oct-2024, HACK just need something to handle our deserialization errors
    // should not be using V1 errors but would be too much to fix this now
    // NOTE: must be after the UnrecognizedPropertyException check
    // 09-Jan-2025, tatu: [data-api#1812] Not ideal but slightly better than before
    if (e instanceof JsonMappingException jme) {
      return RequestException.Code.REQUEST_STRUCTURE_MISMATCH.get(
          Map.of("errorMessage", e.getMessage()));
      //          .getCommandResultError(Response.Status.BAD_REQUEST);
    }

    // Will need to add more handling but start with above
    return handleUnrecognizedException(e, message);
  }

  private static APIException handleUnrecognizedException(Throwable throwable, String message) {

    LOGGER.error(
        String.format(
            "Unrecognized Exception (%s) caught, mapped to SERVER_UNHANDLED_ERROR: %s",
            throwable.getClass().getName(), message),
        throwable);

    return ServerException.Code.UNEXPECTED_SERVER_ERROR.get(ErrorFormatters.errVars(throwable));
  }
}
