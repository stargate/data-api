package io.stargate.sgv2.jsonapi.api.v1.util;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.hasEntry;

import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.exception.*;
import org.hamcrest.Matcher;

public class DataApiResponseValidator {
  protected final ValidatableResponse response;

  public DataApiResponseValidator(ValidatableResponse response) {
    this.response = response;
  }

  // // // Access to Hamcrest Response Matchers // // //

  public ValidatableResponse response() {
    return response;
  }

  public DataApiResponseValidator body(String path, Matcher<?> matcher, Object... args) {
    return new DataApiResponseValidator(response.body(path, matcher, args));
  }

  // // // API-aware validation: status, error codes // // //

  public DataApiResponseValidator hasNoStatus() {
    return body("status", is(nullValue()));
  }

  public DataApiResponseValidator hasNoErrors() {
    return body("errors", is(nullValue()));
  }

  public DataApiResponseValidator hasSingleApiError(ErrorCodeV1 errorCode) {
    return body("errors", hasSize(1))
        .body("errors[0].exceptionClass", is("JsonApiException"))
        .body("errors[0].errorCode", is(errorCode.name()));
  }

  public DataApiResponseValidator hasSingleApiError(ErrorCodeV1 errorCode, String messageSnippet) {
    return hasSingleApiError(errorCode, containsString(messageSnippet));
  }

  public DataApiResponseValidator hasSingleApiError(
      ErrorCodeV1 errorCode, Matcher<String> messageMatcher) {
    return hasSingleApiError(errorCode).body("errors[0].message", messageMatcher);
  }

  public <T extends APIException> DataApiResponseValidator hasSingleApiError(
      ErrorCode<T> errorCode, Class<T> errorClass) {
    return body("errors", hasSize(1))
        .body("errors[0].exceptionClass", is(errorClass.getSimpleName()))
        .body("errors[0].errorCode", is(errorCode.toString()));
  }

  public <T extends APIException> DataApiResponseValidator mayHasSingleApiError(
      ErrorCode<T> errorCode, Class<T> errorClass) {
    if (errorCode == null) {
      return body("errors", is(nullValue()));
    }
    return body("errors", hasSize(1))
        .body("errors[0].exceptionClass", is(errorClass.getSimpleName()))
        .body("errors[0].errorCode", is(errorCode.toString()));
  }

  /**
   * @param errorCode Error code to check for
   * @param errorClass Error class to check for
   * @param messageSnippet Set of pieces of error message to check: ALL must match
   */
  public <T extends APIException> DataApiResponseValidator hasSingleApiError(
      ErrorCode<T> errorCode, Class<T> errorClass, String... messageSnippet) {
    DataApiResponseValidator validator = hasSingleApiError(errorCode, errorClass);
    for (String snippet : messageSnippet) {
      validator = validator.body("errors[0].message", containsString(snippet));
    }
    return validator;
  }

  public <T extends APIException> DataApiResponseValidator hasSingleApiError(
      ErrorCode<T> errorCode, Class<T> errorClass, Matcher<String> messageMatcher) {
    return hasSingleApiError(errorCode, errorClass).body("errors[0].message", messageMatcher);
  }

  // // // API-aware validation: non-error content // // //

  public DataApiResponseValidator hasNoField(String path) {
    return body(path, is(nullValue()));
  }

  public DataApiResponseValidator hasNoData() {
    return body("data", is(nullValue()));
  }

  // can rename later, TODO above method hasNoData seems like a bug.
  public DataApiResponseValidator hasNoDataForTableFind() {
    return body("data.documents", is(empty()));
  }

  public DataApiResponseValidator hasJSONField(String path, String rawJson) {
    return body(path, jsonEquals(rawJson));
  }

  public DataApiResponseValidator hasNoWarnings() {
    return body("warnings", is(nullValue()));
  }

  public DataApiResponseValidator hasSingleWarning(String code) {
    return body("status.warnings", hasSize(1))
        .body(
            "status.warnings[0]",
            hasEntry(ErrorObjectV2Constants.Fields.FAMILY, ErrorFamily.REQUEST.name()))
        .body(
            "status.warnings[0]",
            hasEntry(ErrorObjectV2Constants.Fields.SCOPE, RequestException.Scope.WARNING.scope()))
        .body("status.warnings[0]", hasEntry(ErrorObjectV2Constants.Fields.CODE, code));
  }

  public DataApiResponseValidator hasNoErrorsNoWarnings() {
    return body("errors", is(nullValue())).body("warnings", is(nullValue()));
  }
}
