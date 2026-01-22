package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.*;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.WebApplicationException;
import java.util.Map;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mappers that are bound into the quarkus framework to handle exceptions raised form
 * there.
 */
public class FrameworkExceptionMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrameworkExceptionMapper.class);

  /**
   * Prefix used in constraint violation property paths that should be stripped out. <b>NOTE:</b>
   * This name must match the name of the function in the ResourceHandeler, e.g. {@link
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
  @ServerExceptionMapper
  public RestResponse<CommandResult> mapThrowable(Throwable throwable) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("mapThrowable() - mapping attached exception", throwable);
    }

    var mapped = ThrowableToErrorMapper.mapThrowable(throwable);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("mapThrowable() - mapped to attached exception", mapped);
    }
    return CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
        .addThrowable(mapped)
        .build()
        .toRestResponse();
  }

  /**
   * Mapping for jackson parsing and mapping exceptions
   *
   * <p>Uses the attribute because these two Jackson exceptions do not have a common parent.
   */
  @ServerExceptionMapper({JsonParseException.class, MismatchedInputException.class})
  public RestResponse<CommandResult> mapJacksonException(Throwable jacksonException) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("mapJacksonException() - mapping attached exception", jacksonException);
    }

    /// TODO: Aaron - bring the handling for jackon errors into this class
    var mapped = ThrowableToErrorMapper.mapThrowable(jacksonException);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("mapJacksonException() - mapped to attached exception", mapped);
    }
    return CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
        .addThrowable(mapped)
        .build()
        .toRestResponse();
  }

  /**
   * Mapping for Jakarta WebApplicationException and its subtypes
   *
   * <p>
   */
  @ServerExceptionMapper
  public RestResponse<CommandResult> mapJakartaException(WebApplicationException wae) {

    // 06-Nov-2023, tatu: Let's dig the innermost root cause; needed f.ex for [jsonapi#448]
    //    to get to StreamConstraintsException
    Throwable toReport = wae;
    while (toReport.getCause() != null) {
      toReport = toReport.getCause();
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "mapJakartaException() - wae='{}', translating attached exception", wae, toReport);
    }

    var resultBuilder = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP);

    var restResponse =
        switch (toReport) {
          case APIException ae -> // Already an APIException, nothing to do
              resultBuilder.addThrowable(ae).build().toRestResponse();
          case JsonApiException jae -> resultBuilder.addThrowable(jae).build().toRestResponse();
          case StreamConstraintsException sce ->
              resultBuilder
                  .addThrowable(
                      DocumentException.Code.SHRED_DOC_LIMIT_VIOLATION.get(
                          Map.of("errorMessage", sce.getMessage())))
                  .build()
                  .toRestResponse();
          case NotAllowedException nae -> responseForException(nae);
          case NotFoundException nfe -> responseForException(nfe);
          case NotSupportedException nse ->
              resultBuilder
                  .addThrowable(RequestException.Code.UNSUPPORTED_CONTENT_TYPE.get())
                  .build()
                  .toRestResponse();
          default ->
              resultBuilder
                  .addThrowable(ThrowableToErrorMapper.mapThrowable(toReport))
                  .build()
                  .toRestResponse();
        };

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "mapJakartaException() - returning restResponse.getStatusInfo()={}",
          restResponse.getStatusInfo());
    }
    return restResponse;
  }

  @ServerExceptionMapper
  public RestResponse<CommandResult> constraintViolationException(
      ConstraintViolationException exception) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("constraintViolationException() - mapping attached exception", exception);
    }

    var builder = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP);

    // this used to have a distinct() call, but it was doing it on all CommandError objects which
    // like the ApiException has a unique ID so had not affect.
    exception.getConstraintViolations().stream()
        .map(FrameworkExceptionMapper::constraintException)
        .forEach(builder::addThrowable);

    return builder.build().toRestResponse();
  }

  private static APIException constraintException(ConstraintViolation<?> violation) {

    // Let's remove useless "postCommand." prefix if seen
    // This comes from the name of the function on the ResourceHandler
    var propertyPath =
        violation.getPropertyPath().toString().startsWith(PREFIX_POST_COMMAND)
            ? violation.getPropertyPath().toString().substring(PREFIX_POST_COMMAND.length())
            : violation.getPropertyPath().toString();

    return RequestException.Code.COMMAND_FIELD_VALUE_INVALID.get(
        Map.of(
            "field",
            propertyPath,
            "value",
            prettyPrintConstraintValue(violation.getInvalidValue()),
            "message",
            violation.getMessage()));
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

  private static RestResponse<CommandResult> responseForException(WebApplicationException wae) {
    return RestResponse.status(wae.getResponse().getStatus());
  }
}
