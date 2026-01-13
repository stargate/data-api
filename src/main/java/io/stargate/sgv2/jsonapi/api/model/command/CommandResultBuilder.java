package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.util.recordable.Jsonable;
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
  private final List<CommandErrorV2> cmdErrors = new ArrayList<>();
  private final List<JsonNode> documents = new ArrayList<>();
  // Warnings are always the V2 error structure
  private final List<CommandErrorV2> warnings = new ArrayList<>();

  private String nextPageState = null;

  private final ResponseType responseType;

  private final RequestTracing requestTracing;

  // amorton - we could probably use the same factory for errors and warning, keeping seperate
  // as we have only just refactored this area and to keep the logic clear
  private final CommandErrorFactory cmdErrorFactory =  new CommandErrorFactory();
  private final CommandErrorFactory cmdWarningError =  new CommandErrorFactory();

  CommandResultBuilder(
      ResponseType responseType, RequestTracing requestTracing) {
    this.responseType = responseType;

    // There is a no op implementation for tracing that is used when tracing is disabled
    this.requestTracing = Objects.requireNonNull(requestTracing, "requestTracing must not be null");
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
    return addThrowable(throwable, true);
  }

  public CommandResultBuilder addThrowable(Throwable throwable, boolean andCause) {

    var ret = addCommandError(cmdErrorFactory.create(throwable));
    while (andCause && throwable.getCause() != null) {
      throwable = throwable.getCause();
      ret = ret.addCommandError(cmdErrorFactory.create(throwable));
    }
    return ret;
  }

  public CommandResultBuilder addCommandError(List<CommandErrorV2> errors) {
    Objects.requireNonNull(errors, "errors cannot be null").forEach(this::addCommandError);
    return this;
  }

  public CommandResultBuilder addCommandError(CommandErrorV2 error) {
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
    warnings.add(cmdWarningError.create(warning));
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

    if (!warnings.isEmpty()) {
      cmdStatus.put(CommandStatus.WARNINGS, warnings);
    }

    requestTracing
        .getSession()
        .ifPresent(session -> cmdStatus.put(CommandStatus.TRACE, Jsonable.toJson(session)));

    // null out values that are empty, the CommandResult serializer will ignore them when the JSON
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
          case MULTI_DOCUMENT ->
              // if there are any errors we do not return the response data for multi doc either
              finalErrors == null
                  ? new ResponseData.MultiResponseData(documents, nextPageState)
                  : null;
          case STATUS_ONLY -> null;
        };

    return new CommandResult(responseData, finalStatus, finalErrors);
  }
}
