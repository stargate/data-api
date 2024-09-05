package io.stargate.sgv2.jsonapi.service.operation.collections;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

/** Inherited Exception class to handle retry */
public class LWTException extends JsonApiException {
  public LWTException(ErrorCodeV1 errorCode) {
    super(errorCode);
  }
}
