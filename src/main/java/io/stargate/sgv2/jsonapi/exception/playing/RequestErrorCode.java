package io.stargate.sgv2.jsonapi.exception.playing;

public enum RequestErrorCode {

  FILTER_MULTIPLE_ID_FILTER(),

  FILTER_FIELDS_LIMIT_VIOLATION("Too many fields in the filter");


  public final String title;
  private final String template;

  RequestErrorCode(String title) {
    this.title = // get title from file
    this.template = // get template from file
  }

  Error toError(){

  }
}
