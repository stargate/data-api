package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
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
 * <p>Can serve as an aggregator, using the {@link #aggregate} function. TODO: AARON DOCS
 *
 * @param allInsertions Attempted insertions
 * @param returnDocumentResponses Whether to return detailed document responses
 * @param successfulInsertions Successfully inserted documents, NOTE: this list is mutated after
 *     creation
 * @param failedInsertions Failed insertions NOTE: this list is mutated after creation
 */
public class InsertOperationPage implements Supplier<CommandResult> {

  // TODO: AARON changed from a record because the successfulInsertions and failedInsertions were
  // setable in the ctor
  // but they have to be empty mutable lists
  private final List<? extends InsertAttempt> allInsertions;
  private final boolean returnDocumentResponses;

  // The success and failed lists are mutable and are used to build the response
  // they do not use the ? wild card because they are always added to via {@link #aggregate} which
  // wants to accept only of the InsertAttempt interface
  private final List<InsertAttempt> successfulInsertions;
  private final List<InsertAttempt> failedInsertions;

  public InsertOperationPage(
      List<? extends InsertAttempt> allAttemptedInsertions, boolean returnDocumentResponses) {
    this.allInsertions = List.copyOf(allAttemptedInsertions);
    this.returnDocumentResponses = returnDocumentResponses;

    this.successfulInsertions = new ArrayList<>(allAttemptedInsertions.size());
    this.failedInsertions = new ArrayList<>(allAttemptedInsertions.size());
  }

  enum InsertionStatus {
    OK,
    ERROR,
    SKIPPED
  }

  // TODO AARON - I think this is for the document responses, confirm
  @JsonPropertyOrder({"_id", "status", "errorsIdx"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  record InsertionResult(DocRowIdentifer _id, InsertionStatus status, Integer errorsIdx) {}

  /** {@inheritDoc} */
  @Override
  public CommandResult get() {
    // Sort on the insert position to rebuild the order we of the documents from the insert.
    // used for both legacy and new style output
    Collections.sort(failedInsertions);
    // TODO AARON used to only sort the success list when not returning detaile responses, check OK
    Collections.sort(successfulInsertions);

    if (!returnDocumentResponses) { // legacy output, limited to ids, error messages
      List<CommandResult.Error> errors =
          failedInsertions.isEmpty()
              ? null
              : failedInsertions.stream().map(InsertOperationPage::getOldStyleError).toList();

      // Note: See DocRowIdentifer, it has an attribute that will be called for JSON serialization
      List<DocRowIdentifer> insertedIds =
          successfulInsertions.stream()
              .map(InsertAttempt::docRow)
              .map(WritableDocRow::docRowID)
              .toList();
      return new CommandResult(null, Map.of(CommandStatus.INSERTED_IDS, insertedIds), errors);
    }

    // UPTO THIS AARON

    // New style output: detailed responses.
    InsertionResult[] results = new InsertionResult[allInsertions.size()];
    List<CommandResult.Error> errors = new ArrayList<>();

    // Results array filled in order: first successful insertions
    for (InsertAttempt okInsertion : successfulInsertions) {
      results[okInsertion.position()] =
          new InsertionResult(okInsertion.docRow().docRowID(), InsertionStatus.OK, null);
    }
    // Second: failed insertions; output in order of insertion
    for (InsertAttempt failedInsertion : failedInsertions) {
      // TODO AARON - confirm the null handling in the getError
      CommandResult.Error error = getError(failedInsertion.failure().orElse(null));

      // We want to avoid adding the same error multiple times, so we keep track of the index:
      // either one exists, use it; or if not, add it and use the new index.
      int errorIdx = errors.indexOf(error);
      if (errorIdx < 0) { // new non-dup error; add it
        errorIdx = errors.size(); // will be appended at the end
        errors.add(error);
      }
      results[failedInsertion.position()] =
          new InsertionResult(failedInsertion.docRow().docRowID(), InsertionStatus.ERROR, errorIdx);
    }

    // And third, if any, skipped insertions; those that were not attempted (f.ex due
    // to failure for ordered inserts)
    for (int i = 0; i < results.length; i++) {
      if (null == results[i]) {
        results[i] =
            new InsertionResult(
                allInsertions.get(i).docRow().docRowID(), InsertionStatus.SKIPPED, null);
      }
    }
    return new CommandResult(
        null, Map.of(CommandStatus.DOCUMENT_RESPONSES, Arrays.asList(results)), errors);
  }

  private static CommandResult.Error getOldStyleError(InsertAttempt insertAttempt) {
    String message =
        "Failed to insert document with _id %s: %s"
            .formatted(
                insertAttempt.docRow().docRowID(),
                insertAttempt
                    .failure()
                    .map(Throwable::getMessage)
                    .orElse("InsertAttempt failure was null."));

    /// TODO: confirm the null hanlding in the getMapperWithMessageFunction
    // passing null is what would have happened before changing to optional
    return ThrowableToErrorMapper.getMapperWithMessageFunction()
        .apply(insertAttempt.failure().orElse(null), message);
  }

  private static CommandResult.Error getError(Throwable throwable) {
    // TODO AARON - confirm we have two different error message paths
    return ThrowableToErrorMapper.getMapperWithMessageFunction()
        .apply(throwable, throwable.getMessage());
  }

  /**
   * Aggregates the result of the insert operation into this object.
   *
   * @param insertion Document insertion attempt
   */
  public void aggregate(InsertAttempt insertion) {
    // TODO: AARON: confirm this should not add to the allInsertions list. It would seem better if
    // it did
    insertion
        .failure()
        .ifPresentOrElse(
            throwable -> failedInsertions.add(insertion),
            () -> successfulInsertions.add(insertion));
  }
}
