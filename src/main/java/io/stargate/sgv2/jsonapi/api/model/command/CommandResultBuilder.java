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
  private final List<Throwable> throwables = new ArrayList<>();
  private final List<CommandResult.Error> cmdErrors = new ArrayList<>();
  private final List<JsonNode> documents = new ArrayList<>();
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

  public CommandResultBuilder addThrowable(Throwable error) {
    throwables.add(error);
    return this;
  }

  public CommandResultBuilder addCommandError(CommandResult.Error error) {
    cmdErrors.add(error);
    return this;
  }

  public CommandResultBuilder addDocument(JsonNode document) {
    documents.add(document);
    return this;
  }

  public CommandResultBuilder addDocument(List<JsonNode> documents) {
    this.documents.addAll(documents);
    return this;
  }

  public CommandResultBuilder nextPageState(String nextPageState) {
    this.nextPageState = nextPageState;
    return this;
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

    // null out values that are empty, the CommandResult serialiser will ignore them when the JSON
    // is built
    var finalStatus = cmdStatus.isEmpty() ? null : cmdStatus;
    var finalErrors = finaliseErrors();

    return switch (responseType) {
      case SINGLE_DOCUMENT ->
          new CommandResult(
              new CommandResult.SingleResponseData(
                  documents.isEmpty() ? null : documents.getFirst()),
              finalStatus,
              finalErrors);
      case MULTI_DOCUMENT ->
          new CommandResult(
              new CommandResult.MultiResponseData(documents, nextPageState),
              finalStatus,
              finalErrors);
      case STATUS_ONLY -> new CommandResult(null, finalStatus, finalErrors);
    };
  }

  private List<CommandResult.Error> finaliseErrors() {

    if (!throwables.isEmpty() && !cmdErrors.isEmpty()) {
      throw new IllegalStateException(
          "CommandResultBuilder.build() - both throwables and errors added, use one or the other");
    }

    if (cmdErrors.isEmpty() && throwables.isEmpty()) {
      return null;
    }

    if (!cmdErrors.isEmpty()) {
      return cmdErrors;
    }

    return throwables.stream().map(this::getErrorObject).toList();
  }

  /**
   * Gets the appropriately formatted error given {@link #useErrorObjectV2} and {@link #debugMode}.
   */
  private CommandResult.Error getErrorObject(Throwable throwable) {

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
}