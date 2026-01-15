package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.*;
import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.WebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tries to omit the `WebApplicationException` and just report the cause. <p/ */
public class WebApplicationExceptionMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebApplicationExceptionMapper.class);

  @ServerExceptionMapper
  public RestResponse<CommandResult> translateWebApplicationException(
      WebApplicationException webApplicationException) {

    // 06-Nov-2023, tatu: Let's dig the innermost root cause; needed f.ex for [jsonapi#448]
    //    to get to StreamConstraintsException
    Throwable toReport = webApplicationException;
    while (toReport.getCause() != null) {
      toReport = toReport.getCause();
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "translateWebApplicationException() - webApplicationException='{}', translating attached exception",
          webApplicationException,
          toReport);
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
                      ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(sce.getMessage(), sce))
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
          "translateWebApplicationException() - returning restResponse.getStatusInfo()={}",
          restResponse.getStatusInfo());
    }
    return restResponse;

    //    // and if we have StreamConstraintsException, re-create as ApiException
    //    if (toReport instanceof StreamConstraintsException) {
    //      // but leave out the root cause, as it is not useful
    //      toReport = ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(toReport.getMessage());
    //    }

    //    if (toReport instanceof JsonApiException jae) {
    //      return RestResponse.status(jae.getHttpStatus(), commandResult);
    //    }
    //    // Return 405 for method not allowed and 404 for not found
    //    if (webApplicationException instanceof NotAllowedException) {
    //      return RestResponse.status(RestResponse.Status.METHOD_NOT_ALLOWED, commandResult);
    //    }
    //    if (webApplicationException instanceof NotFoundException) {
    //      return RestResponse.status(RestResponse.Status.NOT_FOUND, commandResult);
    //    }
    //    // Return 415 for invalid Content-Type
    //    if (webApplicationException instanceof NotSupportedException) {
    //      return RestResponse.status(RestResponse.Status.UNSUPPORTED_MEDIA_TYPE, commandResult);
    //    }

    //    return RestResponse.ok(commandResult);
  }

  private static RestResponse<CommandResult> responseForException(WebApplicationException wae) {
    return RestResponse.status(wae.getResponse().getStatus());
  }
}
