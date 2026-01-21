package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

/** TODO: amorton - 21 jan 2026 - to remove see https://github.com/stargate/data-api/issues/2341 */
public class ExceptionUtil {

  public static String getThrowableGroupingKey(Throwable error) {
    return switch (error) {
      case JsonApiException jae -> jae.getErrorCode().name();
      case APIException ae -> ae.code;
      default -> error.getClass().getSimpleName();
    };
  }
}
