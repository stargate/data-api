package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.WebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Tries to omit the `WebApplicationException` and just report the cause. */
public class WebApplicationExceptionMapper {

  @Inject private DebugModeConfig debugModeConfig;
  @Inject private OperationsConfig operationsConfig;

  @ServerExceptionMapper
  public RestResponse<CommandResult> webApplicationExceptionMapper(WebApplicationException e) {
    // 06-Nov-2023, tatu: Let's dig the innermost root cause; needed f.ex for [jsonapi#448]
    //    to get to StreamConstraintsException
    Throwable toReport = e;
    while (toReport.getCause() != null) {
      toReport = toReport.getCause();
    }

    // and if we have StreamConstraintsException, re-create as ApiException
    if (toReport instanceof StreamConstraintsException) {
      // but leave out the root cause, as it is not useful
      toReport = ErrorCodeV1.SHRED_DOC_LIMIT_VIOLATION.toApiException(toReport.getMessage());
    }

    // V2 Error are returned as APIException, this is required to translate the exception to
    // CommandResult if the exception thrown as part of command deserialization
    if (toReport instanceof APIException ae) {
      var errorBuilder =
          new APIExceptionCommandErrorBuilder(
              debugModeConfig.enabled(), operationsConfig.extendError());
      return RestResponse.ok(
          CommandResult.statusOnlyBuilder(false, false)
              .addCommandResultError(errorBuilder.buildLegacyCommandResultError(ae))
              .build());
    }

    CommandResult commandResult = new ThrowableCommandResultSupplier(toReport).get();
    if (toReport instanceof JsonApiException jae) {
      return RestResponse.status(jae.getHttpStatus(), commandResult);
    }
    // Return 405 for method not allowed and 404 for not found
    if (e instanceof NotAllowedException) {
      return RestResponse.status(RestResponse.Status.METHOD_NOT_ALLOWED, commandResult);
    }
    if (e instanceof NotFoundException) {
      return RestResponse.status(RestResponse.Status.NOT_FOUND, commandResult);
    }
    // Return 415 for invalid Content-Type
    if (e instanceof NotSupportedException) {
      return RestResponse.status(RestResponse.Status.UNSUPPORTED_MEDIA_TYPE, commandResult);
    }

    return RestResponse.ok(commandResult);
  }
}
