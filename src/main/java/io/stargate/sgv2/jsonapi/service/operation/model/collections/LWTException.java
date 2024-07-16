package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

/** Inherited Exception class to handle retry */
public class LWTException extends JsonApiException {
  public LWTException(ErrorCode errorCode) {
    super(errorCode);
  }
}
