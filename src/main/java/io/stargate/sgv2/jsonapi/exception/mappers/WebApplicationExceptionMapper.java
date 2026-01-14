package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.WebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Tries to omit the `WebApplicationException` and just report the cause. <p/ */
public class WebApplicationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<CommandResult> webApplicationExceptionMapper(
      WebApplicationException webApplicationException) {

    // 06-Nov-2023, tatu: Let's dig the innermost root cause; needed f.ex for [jsonapi#448]
    //    to get to StreamConstraintsException
    Throwable toReport = webApplicationException;
    while (toReport.getCause() != null) {
      toReport = toReport.getCause();
    }

    var resultBuilder = CommandResult.statusOnlyBuilder(RequestTracing.NO_OP);

    return switch (toReport) {
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
        // Return 415 for invalid Content-Type
      case NotSupportedException
                  nse -> // XXX - amorton CHANGE - this was previusly returning an ENTITY
          responseForException(nse);
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
