package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.*;
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
          case NotAllowedException
                      nae -> // XXX - amorton CHANGE - this was previusly returning an ENTITY with
              // status METHOD_NOT_ALLOWED
              responseForException(nae);
          case NotFoundException
                      nfe -> // XXX - amorton CHANGE - this was previusly returning an ENTITY with
              // status NOT_FOUND
              responseForException(nfe);
            // amorton - 15 jan 2026 - existing code returned 415 and an entity body
          case NotSupportedException nse ->
              resultBuilder
                  .addThrowable(RequestException.Code.UNSUPPORTED_CONTENT_TYPE.get())
                  .build()
                  .toRestResponse();
          case null ->
              resultBuilder
                  .addThrowable(
                      ServerException.Code.INTERNAL_SERVER_ERROR.get(
                          "errorMessage", "WebApplicationExceptionMapper error was null"))
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

  private static RestResponse<CommandResult> responseForException(WebApplicationException wae) {
    return RestResponse.status(wae.getResponse().getStatus());
  }
}
