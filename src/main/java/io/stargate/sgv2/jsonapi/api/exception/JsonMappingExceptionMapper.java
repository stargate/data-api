package io.stargate.sgv2.jsonapi.api.exception;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.ws.rs.ext.Provider;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Exception mapper for failures due to {@link StreamConstraintsException} */
@Provider
public class JsonMappingExceptionMapper {
  @ServerExceptionMapper(JsonMappingException.class)
  public RestResponse<CommandResult> jsonMappingExceptionMapper(Throwable t) {
    // Ok: need to look for root cause to see if it's constraints-violation or not
    while (t.getCause() != null) {
      t = t.getCause();
    }
    if (t instanceof StreamConstraintsException) {
      JsonApiException apiException =
          new JsonApiException(
              ErrorCode.SHRED_DOC_LIMIT_VIOLATION,
              ((StreamConstraintsException) t).getOriginalMessage(),
              t);
      return RestResponse.ok(apiException.get());
    }
    // Otherwise it's something else, let Quarkus handle it
    return null;
  }
}
