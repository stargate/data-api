package io.stargate.sgv2.jsonapi.exception.playing;

public enum RequestErrorCode {

  FILTER_MULTIPLE_ID_FILTER(
      "Cannot have more than one _id equals filter clause"),

  FILTER_FIELDS_LIMIT_VIOLATION("Too many fields in the filter");


  public final String title;

  RequestErrorCode(String title) {
    this.title = title;
  }

}
