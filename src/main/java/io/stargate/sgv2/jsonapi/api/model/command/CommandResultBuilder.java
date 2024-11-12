package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import java.util.*;

/**
 * A basic builder pattern for creating a {@link CommandResult} object, so we create it easily and
 * more reliably.
 *
 * <p>See https://github.com/stargate/data-api/issues/1518 for future changes.
 *
 * <p>This is trying to codify the different ways the CommandResult was created, and it still needs
 * to handle the migration from the old error object to the new error object. It should be improved
 * in the ticket above, and once we finish the migration, we can remove the old error object.
 */
public class CommandResultBuilder {

  public enum ResponseType {
    SINGLE_DOCUMENT,
    MULTI_DOCUMENT,
    STATUS_ONLY;
  }

  private final Map<CommandStatus, Object> cmdStatus = new HashMap<>();
  private final List<CommandResult.Error> cmdErrors = new ArrayList<>();
  private final List<JsonNode> documents = new ArrayList<>();
  // Warnings are always the V2 error structure
  private final List<CommandErrorV2> warnings = new ArrayList<>();

  private String nextPageState = null;

  private final ResponseType responseType;

  // If the debug mode is enabled, errors include the errorclass
  private final boolean debugMode;

  // Created in the Ctor
  private final APIExceptionCommandErrorBuilder apiExceptionToError;

  // another builder, because if we add a warning we want to use the V2 error object
  // but may not be returning V2 errors in the result
  private final APIExceptionCommandErrorBuilder apiWarningToError;

  CommandResultBuilder(ResponseType responseType, boolean debugMode) {
    this.responseType = responseType;
    this.debugMode = debugMode;

    this.apiExceptionToError = new APIExceptionCommandErrorBuilder(debugMode);
    this.apiWarningToError = new APIExceptionCommandErrorBuilder(debugMode);
  }

  public CommandResultBuilder addStatus(CommandStatus status, Object value) {
    cmdStatus.put(status, value);
    return this;
  }

  public CommandResultBuilder addThrowable(List<Throwable> throwables) {
    Objects.requireNonNull(throwables, "throwables cannot be null").forEach(this::addThrowable);
    return this;
  }

  public CommandResultBuilder addThrowable(Throwable throwable) {
    var error = throwableToCommandError(throwable);
    return addCommandResultError(error);
  }

  public CommandResultBuilder addCommandResultError(List<CommandResult.Error> errors) {
    Objects.requireNonNull(errors, "errors cannot be null").forEach(this::addCommandResultError);
    return this;
  }

  public CommandResultBuilder addCommandResultError(CommandResult.Error error) {
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

  public CommandResultBuilder addWarning(APIException warning) {
    warnings.add(
        apiWarningToError.buildCommandErrorV2(
            Objects.requireNonNull(warning, "warning cannot be null")));
    return this;
  }

  public CommandResultBuilder nextPageState(String nextPageState) {
    this.nextPageState = nextPageState;
    return this;
  }

  /** Gets the appropriately formatted error given {@link #debugMode}. */
  public CommandResult.Error throwableToCommandError(Throwable throwable) {

    if (throwable instanceof APIException apiException) {
      // new v2 error object, with family etc.
      // the builder will handle the debug mode and extended errors settings to return a V1 or V2
      // error
      return apiExceptionToError.buildLegacyCommandResultError(apiException);
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
              // if there are any errors we do not return the response data for single doc
              // multi doc can have errors and still return the documents
              finalErrors == null
                  ? new ResponseData.SingleResponseData(
                      documents.isEmpty() ? null : documents.getFirst())
                  : null;
          case MULTI_DOCUMENT -> new ResponseData.MultiResponseData(documents, nextPageState);
          case STATUS_ONLY -> null;
        };

    return new CommandResult(responseData, finalStatus, finalErrors);
  }
}
