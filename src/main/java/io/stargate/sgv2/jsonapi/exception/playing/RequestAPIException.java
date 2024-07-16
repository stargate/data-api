package io.stargate.sgv2.jsonapi.exception.playing;

// TODO: DOC any error when working out how to handle a request from the client
public class RequestAPIException extends APIException{


  public RequestAPIException(RequestErrorCode code, String detail) {
    super(ErrorFamily.REQUEST_ERROR_FAMILY, code.name(), code.title, detail);
  }
}
