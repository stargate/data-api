package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;

public class ExceptionUtil {
  public static String getThrowableGroupingKey(Throwable error) {
    if (error instanceof JsonApiException jae) {
      return jae.getErrorCode().name();
    } else {
      return error.getClass().getSimpleName();
    }
  }

  public static CommandResult.Error getError(
      String messageTemplate, List<DocumentId> documentIds, Throwable throwable) {
    String message = messageTemplate.formatted(documentIds, throwable.getMessage());
    return ThrowableToErrorMapper.getMapperWithMessageFunction().apply(throwable, message);
  }
}
