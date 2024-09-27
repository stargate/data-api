package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CommandResultBuilder {

  public enum ResponseType {
    SINGLE_DOCUMENT,
    MULTI_DOCUMENT,
    STATUS_ONLY;
  }

  private final Map<CommandStatus, Object> cmdStatus = new HashMap<>();
  private final List<CommandResult.Error> cmdErrors = new ArrayList<>();
  private final List<JsonNode> documents = new ArrayList<>();
  private final List<String> warnings = new ArrayList<>();

  private String nextPageState = null;

  private final ResponseType responseType;

  // If the debug mode is enabled, errors include the errorclass
  private final boolean debugMode;

  // Flagged true to include the new error object v2
  private final boolean useErrorObjectV2;

  // Created in the Ctor
  private final Function<APIException, CommandResult.Error> apiExceptionToError;

  public CommandResultBuilder(
      ResponseType responseType, boolean useErrorObjectV2, boolean debugMode) {
    this.responseType = responseType;
    this.useErrorObjectV2 = useErrorObjectV2;
    this.debugMode = debugMode;

    this.apiExceptionToError = new APIExceptionCommandErrorBuilder(debugMode, useErrorObjectV2);
  }

  public CommandResultBuilder addStatus(CommandStatus status, Object value) {
    cmdStatus.put(status, value);
    return this;
  }

  public CommandResultBuilder addThrowable(Throwable thorwable) {
    var error = throwableToCommandError(thorwable);
    return addCommandError(error);
  }

  public CommandResultBuilder addCommandError(CommandResult.Error error) {
    cmdErrors.add(error);
    return this;
  }

  public CommandResultBuilder addDocument(JsonNode document) {
    documents.add(document);
    return this;
  }

  public CommandResultBuilder addDocuments(List<JsonNode> documents) {
    this.documents.addAll(documents);
    return this;
  }

  public CommandResultBuilder addWarning(String warning) {
    warnings.add(warning);
    return this;
  }

  public CommandResultBuilder nextPageState(String nextPageState) {
    this.nextPageState = nextPageState;
    return this;
  }

  /**
   * Gets the appropriately formatted error given {@link #useErrorObjectV2} and {@link #debugMode}.
   */
  public CommandResult.Error throwableToCommandError(Throwable throwable) {

    if (throwable instanceof APIException apiException) {
      // new v2 error object, with family etc.
      // the builder will handle the debug mode and extended errors settings to return a V1 or V2
      // error
      return apiExceptionToError.apply(apiException);
    }

    // the mapper handles error object v2 part
    return ThrowableToErrorMapper.getMapperWithMessageFunction()
        .apply(throwable, throwable.getMessage());
  }

  public CommandResult build() {

    switch (responseType) {
      case SINGLE_DOCUMENT:
        if (documents.size() > 1) {
          throw new IllegalStateException(
              String.format(
                  "%s response type requested but multiple documents added, count=%s",
                  responseType, documents.size()));
        }
        break;
      case STATUS_ONLY:
        if (!documents.isEmpty()) {
          throw new IllegalStateException(
              String.format(
                  "%s response type requested but documents added, count=%s",
                  responseType, documents.size()));
        }
        break;
    }

    if (!warnings.isEmpty()) {
      cmdStatus.put(CommandStatus.WARNINGS, warnings);
    }

    // null out values that are empty, the CommandResult serialiser will ignore them when the JSON
    // is built
    var finalStatus = cmdStatus.isEmpty() ? null : cmdStatus;
    var finalErrors = cmdErrors.isEmpty() ? null : cmdErrors;

    var responseData =
        switch (responseType) {
          case SINGLE_DOCUMENT ->
              documents.isEmpty()
                  ? null
                  : new CommandResult.SingleResponseData(documents.getFirst());
          case MULTI_DOCUMENT -> new CommandResult.MultiResponseData(documents, nextPageState);
          case STATUS_ONLY -> null;
        };

    return new CommandResult(responseData, finalStatus, finalErrors);
  }
}
