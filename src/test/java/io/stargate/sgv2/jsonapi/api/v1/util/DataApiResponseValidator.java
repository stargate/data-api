package io.stargate.sgv2.jsonapi.api.v1.util;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import org.hamcrest.Matcher;

public class DataApiResponseValidator {
  private ValidatableResponse response;

  public DataApiResponseValidator(ValidatableResponse response) {
    this.response = response;
  }

  // // // Access to Hamcrest Response Matchers // // //

  public ValidatableResponse response() {
    return response;
  }

  public DataApiResponseValidator body(String path, Matcher<?> matcher, Object... args) {
    response = response.body(path, matcher, args);
    return this;
  }

  // // // API-aware validation: status, error codes // // //

  public DataApiResponseValidator hasNoStatus() {
    return body("status", is(nullValue()));
  }

  public DataApiResponseValidator hasNoErrors() {
    return body("errors", is(nullValue()));
  }

  public DataApiResponseValidator hasSingleApiError(ErrorCode errorCode) {
    return body("errors", hasSize(1))
        .body("errors[0].exceptionClass", is("JsonApiException"))
        .body("errors[0].errorCode", is(errorCode.name()));
  }

  public DataApiResponseValidator hasSingleApiError(ErrorCode errorCode, String messageSnippet) {
    return hasSingleApiError(errorCode, containsString(messageSnippet));
  }

  public DataApiResponseValidator hasSingleApiError(
      ErrorCode errorCode, Matcher<String> messageMatcher) {
    return hasSingleApiError(errorCode).body("errors[0].message", messageMatcher);
  }

  // // // API-aware validation: non-error content // // //

  public DataApiResponseValidator hasNoField(String path) {
    return body(path, is(nullValue()));
  }

  public DataApiResponseValidator hasData() {
    return hasNoField("data");
  }

  public DataApiResponseValidator hasNoData() {
    return body("data", is(nullValue()));
  }

  public DataApiResponseValidator hasJSONField(String path, String rawJson) {
    return body(path, jsonEquals(rawJson));
  }
}
