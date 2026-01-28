package io.stargate.sgv2.jsonapi.exception.mappers;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.constants.ErrorConstants;
import io.stargate.sgv2.jsonapi.exception.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.*;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mappers that are bound into the quarkus framework to handle exceptions raised form
 * there.
 *
 * <p>Using different functions with decorators to make it a bit clearer what is handled where.
 *
 * <p><b>NOTE:</b>Not exactly sure how quarkus if finding the mappers when subclasses are thrown, be
 * careful making changes specially with {@link #mapJacksonException(Throwable)}
 */
public class FrameworkExceptionMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrameworkExceptionMapper.class);

  /**
   * Prefix used in constraint violation property paths that should be stripped out. <b>NOTE:</b>
   * This name must match the name of the function in the ResourceHandler, e.g. {@link
   * io.stargate.sgv2.jsonapi.api.v1.CollectionResource#postCommand(CollectionCommand, String,
   * String)}
   */
  private static final String PREFIX_POST_COMMAND = "postCommand.";

  /**
   * For constraint errors, the maximum length of "value" to include without truncation: values
   * longer than this will be truncated to this length (plus marker)
   */
  private static final int MAX_VALUE_LENGTH_TO_INCLUDE = 1000;

  /**
   * Most generic mapping for things not handled by other functions.
   *
   * <p>This could include ApiExceptions that we throw from inside our deserialization code called
   * from quarkus.
   */
  @ServerExceptionMapper({Throwable.class})
  public RestResponse<CommandResult> mapThrowable(Throwable throwable) {

    var translated = translateThrowable(throwable);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "mapThrowable() - mapped to attached exception throwable.class={}, throwable.message={}",
          classSimpleName(throwable),
          throwable.getMessage(),
          translated);
    }

    return responseForException(translated);
  }

  /**
   * Mapping for jackson parsing and mapping exceptions
   *
   * <p><b>NOTE:</b> This needs to have the specific exception classes listed to be called by
   * quarkus. If not, the {@link com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException}
   * will not be handled.
   */
  @ServerExceptionMapper({JsonParseException.class, MismatchedInputException.class})
  public RestResponse<CommandResult> mapJacksonException(Throwable jacksonException) {

    var translated = translateThrowable(jacksonException);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "mapJacksonException() - mapped to attached exception jacksonException.class={}, jacksonException.message={}",
          classSimpleName(jacksonException),
          jacksonException.getMessage(),
          translated);
    }
    return responseForException(translated);
  }

  /**
   * Mapping for Jakarta WebApplicationException and its subtypes
   *
   * <p>
   */
  @ServerExceptionMapper({WebApplicationException.class})
  public RestResponse<CommandResult> mapJakartaException(
      WebApplicationException webApplicationException) {

    // 06-Nov-2023, tatu: Let's dig the innermost root cause; needed f.ex for [jsonapi#448]
    //    to get to StreamConstraintsException
    Throwable toReport = webApplicationException;
    while (toReport.getCause() != null) {
      toReport = toReport.getCause();
    }

    if (toReport != webApplicationException && LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "mapJakartaException() - processing cause of original exception attached,  webApplicationException.class={}, webApplicationException.message={}",
          classSimpleName(webApplicationException),
          webApplicationException.getMessage(),
          toReport);
    }

    var translated = translateThrowable(toReport);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "mapJakartaException() - mapped to attached exception toReport.class={}, toReport.message={}",
          classSimpleName(toReport),
          toReport.getMessage(),
          translated);
    }
    return responseForException(translated);
  }

  @ServerExceptionMapper({ConstraintViolationException.class})
  public RestResponse<CommandResult> constraintViolationException(
      ConstraintViolationException exception) {

    var builder = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP);

    // this used to have a distinct() call, but it was doing it on all CommandError objects which
    // like the ApiException has a unique ID so had not affect.
    // LOGGING is in apiExceptionFor()
    exception.getConstraintViolations().stream()
        .map(FrameworkExceptionMapper::apiExceptionFor)
        .forEach(builder::addThrowable);

    return builder.build().toRestResponse();
  }

  private static RestResponse<CommandResult> responseForException(RuntimeException exception) {

    return switch (exception) {
      case ClientErrorException cee -> RestResponse.status(cee.getResponse().getStatus());
      default ->
          CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
              .addThrowable(exception)
              .build()
              .toRestResponse();
    };
  }

  /**
   * Translate a Throwable into an appropriate APIException or JsonApiException.
   *
   * <p>NOTES: this code is refactored from the old ThrowableToErrorMapper class, it is missing
   * specific handling befor below because I could not see how they triggered(amorton 26 jan 2026):
   *
   * <ul>
   *   <li>{@link io.quarkus.security.UnauthorizedException}
   *   <li>{@link java.util.concurrent.TimeoutException}
   * </ul>
   *
   * @param throwable
   * @return
   */
  public static RuntimeException translateThrowable(Throwable throwable) {
    return switch (throwable) {
      case APIException ae -> ae;
      case JsonApiException jae -> jae;

        // ##########
        // WEIRD ONES FROM throwableToErrorMapper
        // TODO: AARON - why would this happen ? was in old ThrowableToErrorMapper
      case TimeoutException te -> ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException();

        // ##########
        // # QUARKUS ERRORS
        // these are the client 4xx errors , no change means we return them as-is e.g. the 4XX code
      case NotAllowedException na -> na;
      case NotFoundException nf -> nf;
      case NotSupportedException nse -> RequestException.Code.UNSUPPORTED_CONTENT_TYPE.get();

        // ##########
        // JACKSON ERRORS

      case StreamConstraintsException sce ->
          DocumentException.Code.SHRED_DOC_LIMIT_VIOLATION.get(
              Map.of("errorMessage", sce.getMessage()));

        // Low-level parsing problem? Actual BAD_REQUEST (400) since we could not process
      case JsonParseException jpe ->
          RequestException.Code.REQUEST_NOT_JSON.get(
              Map.of(ErrorConstants.TemplateVars.ERROR_MESSAGE, jpe.getMessage()));

        // Unrecognized property? (note: CommandObjectMapperHandler handles some cases)
        // 09-Oct-2025, tatu: Retain custom exception message, if set by us:
      case UnrecognizedPropertyException upe when ColumnDesc.class.equals(
              upe.getReferringClass()) ->
          RequestException.Code.COMMAND_FIELD_UNKNOWN.get(
              Map.of("field", upe.getPropertyName(), "message", upe.getOriginalMessage()));

        // otherwise rewrite to avoid Jackson-isms:
      case UnrecognizedPropertyException upe -> {
        var knownIds =
            Optional.ofNullable(upe.getKnownPropertyIds()).orElse(Collections.emptyList());
        var knownDesc =
            knownIds.stream()
                .map(ob -> String.format("'%s'", ob.toString()))
                .sorted()
                .collect(Collectors.joining(", "));
        yield RequestException.Code.COMMAND_FIELD_UNKNOWN.get(
            Map.of(
                "field",
                upe.getPropertyName(),
                "message",
                "not one of known fields (%s)".formatted(knownDesc)));
      }

        // NOTE: must be after the UnrecognizedPropertyException check
        // 09-Jan-2025, tatu: [data-api#1812] Not ideal but slightly better than before
      case JsonMappingException jme ->
          RequestException.Code.REQUEST_STRUCTURE_MISMATCH.get(
              Map.of("errorMessage", jme.getMessage()));

        // ##########
        // DEFAULT
      default -> {
        var e =
            ServerException.Code.UNEXPECTED_SERVER_ERROR.get(ErrorFormatters.errVars(throwable));
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error(
              "translateThrowable() - Unrecognized exception translated to attached SERVER_UNHANDLED_ERROR. throwable.class={}, throwable.message={}",
              classSimpleName(throwable),
              throwable.getMessage(),
              e);
        }
        yield e;
      }
    };
  }

  private static APIException apiExceptionFor(ConstraintViolation<?> violation) {

    // Let's remove useless "postCommand." prefix if seen
    // This comes from the name of the function on the ResourceHandler
    var propertyPath =
        violation.getPropertyPath().toString().startsWith(PREFIX_POST_COMMAND)
            ? violation.getPropertyPath().toString().substring(PREFIX_POST_COMMAND.length())
            : violation.getPropertyPath().toString();

    var exception =
        RequestException.Code.COMMAND_FIELD_VALUE_INVALID.get(
            Map.of(
                "field",
                propertyPath,
                "value",
                prettyPrintConstraintValue(violation.getInvalidValue()),
                "message",
                violation.getMessage()));

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "apiExceptionFor() - mapped constraint to attached exception violation.getPropertyPath={}, violation.getMessage={}",
          violation.getPropertyPath(),
          violation.getMessage(),
          exception);
    }
    return exception;
  }

  private static String prettyPrintConstraintValue(Object rawValue) {

    // Note, previous check for rawValue.getClass().isPrimitive() removed as
    // this would be passed in as the boxed type not primitive.

    return switch (rawValue) {
      case null -> "`null`";
      case Number n -> n.toString();
      case Boolean b -> b.toString();
      case JsonNode j -> "<JSON value of " + j.toString().length() + " characters>";
      case Object o when o.toString().length() <= MAX_VALUE_LENGTH_TO_INCLUDE ->
          String.format("\"%s\"", o);
      case Object o ->
          String.format(
              "\"%s\"...[TRUNCATED from %d to %d characters]",
              o.toString().substring(0, MAX_VALUE_LENGTH_TO_INCLUDE),
              o.toString().length(),
              MAX_VALUE_LENGTH_TO_INCLUDE);
    };
  }
}
