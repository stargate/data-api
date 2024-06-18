package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.smallrye.mutiny.tuples.Tuple2;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The internal to insert operation results, keeping ids of successfully and not-successfully
 * inserted documents.
 *
 * <p>Can serve as an aggregator, using the {@link #aggregate} function.
 *
 * @param successfulInsertions Documents that we successfully inserted.
 * @param failedInsertions Documents that failed to be inserted, along with failure reason.
 */
public record InsertOperationPage(
    List<InsertOperation.WritableDocAndPosition> allAttemptedInsertions,
    boolean returnDocumentResponses,
    List<InsertOperation.WritableDocAndPosition> successfulInsertions,
    List<Tuple2<InsertOperation.WritableDocAndPosition, Throwable>> failedInsertions)
    implements Supplier<CommandResult> {
  enum InsertionStatus {
    OK,
    ERROR,
    SKIPPED
  }

  @JsonPropertyOrder({"_id", "status", "errorsIdx"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  record InsertionResult(DocumentId _id, InsertionStatus status, Integer errorsIdx) {}

  /** No-arg constructor, usually used for aggregation. */
  public InsertOperationPage(
      List<InsertOperation.WritableDocAndPosition> allAttemptedInsertions,
      boolean returnDocumentResponses) {
    this(allAttemptedInsertions, returnDocumentResponses, new ArrayList<>(), new ArrayList<>());
  }

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // Ensure insertions and errors are in the input order (wrt unordered insertions),
    // regardless of output format
    Collections.sort(successfulInsertions);
    if (!failedInsertions().isEmpty()) {
      Collections.sort(
          failedInsertions, Comparator.comparing(tuple -> tuple.getItem1().position()));
    }

    if (!returnDocumentResponses()) { // legacy output, limited to ids, error messages
      List<CommandResult.Error> errors;
      if (failedInsertions().isEmpty()) {
        errors = null;
      } else {
        errors =
            failedInsertions.stream()
                .map(tuple -> getError(tuple.getItem1().document().id(), tuple.getItem2()))
                .toList();
      }
      // Old style, simple ids:
      List<DocumentId> insertedIds =
          successfulInsertions.stream().map(docAndPos -> docAndPos.document().id()).toList();
      return new CommandResult(null, Map.of(CommandStatus.INSERTED_IDS, insertedIds), errors);
    }

    // New style output: detailed responses.
    InsertionResult[] results = new InsertionResult[allAttemptedInsertions().size()];
    List<CommandResult.Error> errors = new ArrayList<>();

    // Results array filled in order: first successful insertions
    for (InsertOperation.WritableDocAndPosition docAndPos : successfulInsertions) {
      results[docAndPos.position()] =
          new InsertionResult(docAndPos.document().id(), InsertionStatus.OK, null);
    }
    // Second: failed insertions
    for (Tuple2<InsertOperation.WritableDocAndPosition, Throwable> failed : failedInsertions) {
      InsertOperation.WritableDocAndPosition docAndPos = failed.getItem1();
      Throwable throwable = failed.getItem2();
      CommandResult.Error error = getError(throwable);
      int errorIdx = errors.indexOf(error);
      if (errorIdx < 0) { // not yet added, add
        errorIdx = errors.size();
        errors.add(error);
      }
      int idx = docAndPos.position();
      results[idx] =
          new InsertionResult(docAndPos.document().id(), InsertionStatus.ERROR, errorIdx);
    }
    // And third, if any, skipped
    for (int i = 0; i < results.length; i++) {
      if (null == results[i]) {
        results[i] =
            new InsertionResult(
                allAttemptedInsertions.get(i).document().id(), InsertionStatus.SKIPPED, null);
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
   * @param docWithPosition Document that was inserted (or failed to be inserted)
   * @param failure If not null, means an error occurred during attempted insertion
   */
  public void aggregate(InsertOperation.WritableDocAndPosition docWithPosition, Throwable failure) {
    if (null == failure) {
      successfulInsertions.add(docWithPosition);
    } else {
      failedInsertions.add(Tuple2.of(docWithPosition, failure));
    }
  }
}
