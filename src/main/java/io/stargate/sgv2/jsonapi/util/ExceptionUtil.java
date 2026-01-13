package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.util.List;

public class ExceptionUtil {

  public static String getThrowableGroupingKey(Throwable error) {
    return switch (error) {
      case JsonApiException jae -> jae.getErrorCode().name();
      case APIException ae -> ae.code;
      default -> error.getClass().getSimpleName();
    };
  }

//  public static CommandResult.Error getError(
//      String messageTemplate, List<DocumentId> documentIds, Throwable throwable) {
//
//    if (throwable instanceof APIException apiException) {
//      // AJM - GH #2309 - this is a hack until all of V1 errors and ThrowableToErrorMapper are
//      // removed
//      // if we know it is a V2 error it has lots of structure, and the message is long.
//      // So we will want to prefix the TITLE of the error with our message template
//
//      // TERRIBLE HACK - when this function is called with an APIException we ignore the message
//      // template
//      var commandError =
//          ThrowableToErrorMapper.getMapperWithMessageFunction().apply(apiException, "");
//      commandError
//          .fields()
//          .put(
//              ErrorObjectV2Constants.Fields.TITLE,
//              messageTemplate.formatted(
//                  documentIds, commandError.fields().get(ErrorObjectV2Constants.Fields.TITLE)));
//      return commandError;
//    }
//
//    String message = messageTemplate.formatted(documentIds, throwable.getMessage());
//    return ThrowableToErrorMapper.getMapperWithMessageFunction().apply(throwable, message);
//  }
}
