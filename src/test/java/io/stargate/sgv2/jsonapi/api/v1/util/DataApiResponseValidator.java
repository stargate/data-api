package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.hasEntry;

import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.exception.*;
import java.util.Map;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class DataApiResponseValidator {
  protected final ValidatableResponse response;
  protected final Command.CommandName commandName;

  private final TypeSafeMatcher<Map<String, ?>> responseIsSuccess;
  private final TypeSafeMatcher<Map<String, ?>> responseIsError;

  public DataApiResponseValidator(Command.CommandName commandName, ValidatableResponse response) {
    this.commandName = commandName;
    this.response = response;

    this.responseIsError =
        switch (commandName) {
          case DROP_TABLE, DROP_INDEX, CREATE_INDEX, CREATE_TABLE, ALTER_TABLE, LIST_INDEXES ->
              responseIsErrorWithOptionalStatus();
          default -> responseIsError();
        };
    this.responseIsSuccess =
        switch (commandName) {
          case FIND_ONE, FIND -> responseIsFindSuccessOptionalStatus();
          case INSERT_ONE, INSERT_MANY, UPDATE_ONE, UPDATE_MANY, DELETE_ONE, DELETE_MANY ->
              responseIsWriteSuccess();
          case ALTER_TABLE,
                  CREATE_TABLE,
                  DROP_TABLE,
                  CREATE_INDEX,
                  DROP_INDEX,
                  CREATE_VECTOR_INDEX,
                  LIST_TABLES,
                  LIST_INDEXES ->
              responseIsDDLSuccess();
          case CREATE_COLLECTION -> responseIsDDLSuccess();
          default ->
              throw new IllegalArgumentException(
                  "DataApiResponseValidator: Unexpected command name: " + commandName);
        };
  }

  // // // Access to Hamcrest Response Matchers // // //

  public ValidatableResponse response() {
    return response;
  }

  public DataApiResponseValidator body(String path, Matcher<?> matcher, Object... args) {
    return new DataApiResponseValidator(commandName, response.body(path, matcher, args));
  }

  // // // High level command aware validation // // //

  /**
   * If the <code>errorCode</code> is null runs {@link #wasSuccessful()} otherwise uses the params
   * to call {@link #hasSingleApiError(ErrorCode, Class, String...)}
   */
  public <T extends APIException> DataApiResponseValidator wasSuccessfulOrError(
      ErrorCode<T> errorCode, Class<T> errorClass, String... messageSnippet) {
    if (errorCode == null) {
      return wasSuccessful();
    } else {
      return hasSingleApiError(errorCode, errorClass, messageSnippet);
    }
  }

  /**
   * Checks the structure of the response was as expected whe the command is successful.
   *
   * <p><b>NOTE:</b> This does not check the data in the response, i.e. does not check the doc ID in
   * an insert result, you need to do that.
   */
  public DataApiResponseValidator wasSuccessful() {

    var msg = "wasSuccessful() for %s ".formatted(commandName);
    switch (commandName) {
      case FIND_ONE, FIND -> {
        return hasNoErrors();
      }
      case INSERT_ONE -> {
        return hasNoErrors().body("status.insertedIds", hasSize(1));
      }
      case INSERT_MANY -> {
        return hasNoErrors();
      }
      case DELETE_ONE, DELETE_MANY -> {
        return hasNoErrors();
      }
      case ALTER_TABLE, CREATE_TABLE, DROP_TABLE, CREATE_INDEX, DROP_INDEX, CREATE_VECTOR_INDEX -> {
        return hasNoErrors().hasStatusOK();
      }
      case LIST_TABLES -> {
        return hasNoErrors();
      }
      case LIST_INDEXES -> {
        return hasNoErrors();
      }
      case CREATE_COLLECTION -> {
        return hasNoErrors().hasStatusOK();
      }
      case UPDATE_ONE -> {
        return hasNoErrors();
      }
      default ->
          throw new IllegalArgumentException(
              "DataApiResponseValidator: Unexpected command name: " + commandName);
    }
  }

  // // // API-aware validation: status, error codes // // //

  public DataApiResponseValidator hasNoErrors() {
    return body("$", responseIsSuccess);
  }

  public DataApiResponseValidator hasSingleApiError(ErrorCodeV1 errorCode) {
    return body("$", responseIsError)
        .body("errors", hasSize(1))
        .body("errors[0].exceptionClass", is("JsonApiException"))
        .body("errors[0].errorCode", is(errorCode.name()));
  }

  public DataApiResponseValidator hasSingleApiError(ErrorCodeV1 errorCode, String messageSnippet) {
    return hasSingleApiError(errorCode, containsString(messageSnippet));
  }

  // aaron 19-oct-2024 added wheile redoing a lot of errors, we still need to cleanup the error code
  // world
  public DataApiResponseValidator hasSingleApiError(String errorCode, String messageSnippet) {
    return body("$", responseIsError)
        .body("errors", hasSize(1))
        .body("errors[0].errorCode", is(errorCode))
        .body("errors[0].message", containsString(messageSnippet));
  }

  public DataApiResponseValidator hasSingleApiError(
      ErrorCodeV1 errorCode, Matcher<String> messageMatcher) {
    return hasSingleApiError(errorCode).body("errors[0].message", messageMatcher);
  }

  public <T extends APIException> DataApiResponseValidator hasSingleApiError(
      ErrorCode<T> errorCode, Class<T> errorClass) {

    return body("$", responseIsError)
        .body("errors", hasSize(1))
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

  public <T extends APIException> DataApiResponseValidator hasSingleApiException(T expected) {

    // TODO: aaron 19-oct-2024 this is a bit of a hack, will build ticket to refector the matchers
    // for errors
    return body("$", responseIsError)
        .body("errors", hasSize(1))
        .body("errors[0].errorCode", is(expected.code))
        .body("errors[0].message", is(expected.body))
        .body("errors[0].exceptionClass", is(expected.getClass().getSimpleName()));
  }

  // // // API-aware validation: non-error content // // //

  public DataApiResponseValidator hasNoField(String path) {
    return body(path, is(nullValue()));
  }

  public DataApiResponseValidator hasJSONField(String path, String rawJson) {
    return body(path, jsonEquals(rawJson));
  }

  public DataApiResponseValidator hasNoWarnings() {
    return body("status.warnings", is(nullValue()));
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

  public DataApiResponseValidator hasStatusOK() {
    return body("status.ok", is(1));
  }

  // // // Insert Command Validation // // //
  public DataApiResponseValidator hasInsertedIdCount(int count) {
    return body("status.insertedIds", hasSize(count));
  }

  // // // Read Command Validation // // //

  public DataApiResponseValidator hasEmptyDataDocuments() {
    return body("data.documents", is(empty()));
  }

  public DataApiResponseValidator hasDocuments(int size) {
    return body("data.documents", hasSize(size));
  }

  public DataApiResponseValidator hasDocumentInPosition(int position, String documentJSON) {
    return body("data.documents[%s]".formatted(position), jsonEquals(documentJSON));
  }

  public DataApiResponseValidator hasIndexes(String... indexes) {
    return body("status.indexes", hasSize(indexes.length))
        .body("status.indexes", containsInAnyOrder(indexes));
  }

  public DataApiResponseValidator shouldNotHaveIndexes(String... indexes) {
    return body("status.indexes", not(containsInAnyOrder(indexes)));
  }
}
