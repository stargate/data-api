package io.stargate.sgv2.jsonapi.exception.playing;

//* TODO DOC any error parsing the filter clause in a request
public class FilterValidationException extends RequestAPIException{

  public FilterValidationException(RequestErrorCode code, String detail) {
    super(code, detail);
  }
}
