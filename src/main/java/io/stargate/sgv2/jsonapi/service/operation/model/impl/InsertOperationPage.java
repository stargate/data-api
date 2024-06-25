package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The internal to insert operation results, keeping ids of successfully and not-successfully
 * inserted documents.
 *
 * <p>Can serve as an aggregator, using the {@link #aggregate} function.
 *
 * @param allInsertions Attempted insertions
 */
public record InsertOperationPage(
    List<InsertOperation.InsertAttempt> allInsertions,
    boolean returnDocumentResponses,
    List<InsertOperation.InsertAttempt> successfulInsertions,
    List<InsertOperation.InsertAttempt> failedInsertions)
    implements Supplier<CommandResult> {
  enum InsertionStatus {
    OK,
    ERROR,
    SKIPPED
  }

  @JsonPropertyOrder({"_id", "status", "errorsIdx"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  record InsertionResult(DocumentId _id, InsertionStatus status, Integer errorsIdx) {}

  public InsertOperationPage(
      List<InsertOperation.InsertAttempt> allAttemptedInsertions, boolean returnDocumentResponses) {
    this(allAttemptedInsertions, returnDocumentResponses, new ArrayList<>(), new ArrayList<>());
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    if (!returnDocumentResponses()) { // legacy output, limited to ids, error messages
      List<CommandResult.Error> errors;
      if (failedInsertions.isEmpty()) {
        errors = null;
      } else {
        Collections.sort(failedInsertions);
        errors =
            failedInsertions.stream()
                .map(insertion -> getError(insertion.documentId, insertion.failure))
                .toList();
      }
      // Old style, simple ids:
      Collections.sort(successfulInsertions);
      List<DocumentId> insertedIds =
          successfulInsertions.stream().map(insertion -> insertion.documentId).toList();
      return new CommandResult(null, Map.of(CommandStatus.INSERTED_IDS, insertedIds), errors);
    }

    // New style output: detailed responses.
    InsertionResult[] results = new InsertionResult[allInsertions().size()];
    List<CommandResult.Error> errors = new ArrayList<>();

    // Results array filled in order: first successful insertions
    for (InsertOperation.InsertAttempt okInsertion : successfulInsertions) {
      results[okInsertion.position] =
          new InsertionResult(okInsertion.documentId, InsertionStatus.OK, null);
    }
    // Second: failed insertions; output in order of insertion
    Collections.sort(failedInsertions);
    for (InsertOperation.InsertAttempt failedInsertion : failedInsertions) {
      Throwable throwable = failedInsertion.failure;
      CommandResult.Error error = getError(throwable);

      // We want to avoid adding the same error multiple times, so we keep track of the index:
      // either one exists, use it; or if not, add it and use the new index.
      int errorIdx = errors.indexOf(error);
      if (errorIdx < 0) { // new non-dup error; add it
        errorIdx = errors.size(); // will be appended at the end
        errors.add(error);
      }
      results[failedInsertion.position] =
          new InsertionResult(failedInsertion.documentId, InsertionStatus.ERROR, errorIdx);
    }
    // And third, if any, skipped insertions; those that were not attempted (f.ex due
    // to failure for ordered inserts)
    for (int i = 0; i < results.length; i++) {
      if (null == results[i]) {
        results[i] =
            new InsertionResult(allInsertions.get(i).documentId, InsertionStatus.SKIPPED, null);
      }
    }
    return new CommandResult(
        null, Map.of(CommandStatus.DOCUMENT_RESPONSES, Arrays.asList(results)), errors);
  }

  private static CommandResult.Error getError(DocumentId documentId, Throwable throwable) {
    String message =
        "Failed to insert document with _id %s: %s".formatted(documentId, throwable.getMessage());
    return ThrowableToErrorMapper.getMapperWithMessageFunction().apply(throwable, message);
  }

  private static CommandResult.Error getError(Throwable throwable) {
    return ThrowableToErrorMapper.getMapperWithMessageFunction()
        .apply(throwable, throwable.getMessage());
  }

  /**
   * Aggregates the result of the insert operation into this object.
   *
   * @param insertion Document insertion attempt
   */
  public void aggregate(InsertOperation.InsertAttempt insertion) {
    if (insertion.failure == null) {
      successfulInsertions.add(insertion);
    } else {
      failedInsertions.add(insertion);
    }
  }
}
