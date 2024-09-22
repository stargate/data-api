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

  private final Map<CommandStatus, Object> cmdStatus = new HashMap<>();
  private final List<Throwable> throwables = new ArrayList<>();
  private final List<CommandResult.Error> cmdErrors = new ArrayList<>();
  private final List<JsonNode> documents = new ArrayList<>();
  private String nextPageState = null;

  private final boolean singleDocumentResponse;

  // If the debug mode is enabled, errors include the errorclass
  private final boolean debugMode;

  // Flagged true to include the new error object v2
  private final boolean useErrorObjectV2;

  // Created in the Ctor
  private final Function<APIException, CommandResult.Error> apiExceptionToError;

  public CommandResultBuilder(
      boolean singleDocumentResponse, boolean useErrorObjectV2, boolean debugMode) {
    this.singleDocumentResponse = singleDocumentResponse;
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

    var finalStatus = cmdStatus.isEmpty() ? null : cmdStatus;

    var finalErrors = finaliseErrors();

    if (singleDocumentResponse) {
      if (documents.size() > 1) {
        throw new IllegalStateException(
            "Single document response requested but multiple documents added, count="
                + documents.size());
      }
    }
    var responseData =
        singleDocumentResponse
            ? new CommandResult.SingleResponseData(
                documents.isEmpty() ? null : documents.getFirst())
            : new CommandResult.MultiResponseData(documents, nextPageState);

    return new CommandResult(responseData, finalStatus, finalErrors);
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
