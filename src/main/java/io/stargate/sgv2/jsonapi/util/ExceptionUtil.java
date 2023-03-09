package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExceptionUtil {
  public static String getThrowableGroupingKey(Throwable error) {
    String key = error.getClass().getSimpleName();
    if (error instanceof JsonApiException jae) key = jae.getErrorCode().name();
    return key;
  }

  public static CommandResult.Error getError(
      String messageTemplate, List<DocumentId> documentIds, Throwable throwable) {
    String message = messageTemplate.formatted(documentIds, throwable.getMessage());
    Map<String, Object> fields = new HashMap<>();
    fields.put("exceptionClass", throwable.getClass().getSimpleName());
    if (throwable instanceof JsonApiException jae) {
      fields.put("errorCode", jae.getErrorCode().name());
    }
    return new CommandResult.Error(message, fields);
  }
}
