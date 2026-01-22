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
 * try simplifying as I work to remove this class completely.
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

    if (throwable instanceof APIException) {
      return throwable;
    }

    // if our own V1 exception, shortcut
    if (throwable instanceof JsonApiException) {
      return throwable;
    }

    // General Exception related to JSON handling, thrown by Jackson
    if (throwable instanceof JacksonException jacksonE) {
      return handleJsonProcessingException(jacksonE, message);
    }

    // UnauthorizedException from quarkus
    // TODO: AARON - why would this happen ?
    if (throwable instanceof UnauthorizedException) {
      return APISecurityException.Code.UNAUTHENTICATED_REQUEST.get();
    }

    // TimeoutException from quarkus
    // TODO: AARON - why would this happen ?
    if (throwable instanceof TimeoutException) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException();
    }

    // handle an invalid Content-Type header
    if (throwable instanceof NotSupportedException nse) {
      // validate the Content-Type header, 415 if failed
      return RequestException.Code.UNSUPPORTED_CONTENT_TYPE.get("message", nse.getMessage());
    }

    // handle all other exceptions
    return handleUnrecognizedException(throwable, message);
  }

  private static APIException handleJsonProcessingException(JacksonException e, String message) {

    if (e instanceof JsonParseException) {
      // Low-level parsing problem? Actual BAD_REQUEST (400) since we could not process
      return RequestException.Code.REQUEST_NOT_JSON.get(Map.of("errorMessage", e.getMessage()));
    }

    // Unrecognized property? (note: CommandObjectMapperHandler handles some cases)
    if (e instanceof UnrecognizedPropertyException upe) {

      // 09-Oct-2025, tatu: Retain custom exception message, if set by us:
      if (ColumnDesc.class.equals(upe.getReferringClass())) {
        return RequestException.Code.COMMAND_FIELD_UNKNOWN.get(
            Map.of("field", upe.getPropertyName(), "message", upe.getOriginalMessage()));
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
    }

    // aaron 28-oct-2024, HACK just need something to handle our deserialization errors
    // should not be using V1 errors but would be too much to fix this now
    // NOTE: must be after the UnrecognizedPropertyException check
    // 09-Jan-2025, tatu: [data-api#1812] Not ideal but slightly better than before
    if (e instanceof JsonMappingException jme) {
      return RequestException.Code.REQUEST_STRUCTURE_MISMATCH.get(
          Map.of("errorMessage", e.getMessage()));
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
