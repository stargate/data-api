package io.stargate.sgv2.jsonapi.service.operation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Builds the response for an insert operation or one or more {@link InsertAttempt}s.
 *
 * <p>Keeps track of the inserts, their success status, and then builds the {@link CommandResult}
 * via {@link #get()}.
 *
 * <p>Create an instance with all the insert attempts the insert operation will process, then call
 * {@link #registerCompletedAttempt} for each completed attempt, the instance will then track failed
 * and successful attempts. This is used as an aggregator for {@link
 * io.smallrye.mutiny.groups.MultiCollect#in(Supplier, BiConsumer)}
 */
public class InsertOperationPage implements Supplier<CommandResult> {

  // All the insertions that are going to be attempted
  private final List<? extends InsertAttempt> allInsertions;

  // True if the response should include detailed info for each document
  private final boolean returnDocumentResponses;

  // The success and failed lists are mutable and are used to build the response
  // they do not use the ? wild card because they are always added to via {@link #aggregate} which
  // wants to accept only of the InsertAttempt interface
  private final List<InsertAttempt> successfulInsertions;
  private final List<InsertAttempt> failedInsertions;

  // If the debug mode is enabled, errors include the errorclass
  private final boolean debugMode;

  // Flagged true to include the new error object v2
  private final boolean useErrorObjectV2;

  // Created in the Ctor
  private final Function<APIException, CommandResult.Error> apiExceptionToError;

  /** Create an instance that has debug false and useErrorIbhectV2 false */
  public InsertOperationPage(
      List<? extends InsertAttempt> allAttemptedInsertions, boolean returnDocumentResponses) {
    this(allAttemptedInsertions, returnDocumentResponses, false, false);
  }

  /**
   * Create an instance with the given parameters
   *
   * @param allAttemptedInsertions All the insertions that are going to be attempted.
   * @param returnDocumentResponses If the response should include detailed info for each document.
   * @param debugMode If the debug mode is enabled, errors include the errorclass.
   * @param useErrorObjectV2 Flagged true to include the new error object v2.
   */
  public InsertOperationPage(
      List<? extends InsertAttempt> allAttemptedInsertions,
      boolean returnDocumentResponses,
      boolean debugMode,
      boolean useErrorObjectV2) {

    this.allInsertions = List.copyOf(allAttemptedInsertions);
    this.returnDocumentResponses = returnDocumentResponses;

    this.successfulInsertions = new ArrayList<>(allAttemptedInsertions.size());
    this.failedInsertions = new ArrayList<>(allAttemptedInsertions.size());
    this.debugMode = debugMode;
    this.useErrorObjectV2 = useErrorObjectV2;
    this.apiExceptionToError = new APIExceptionCommandErrorBuilder(debugMode, useErrorObjectV2);
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
    // TODO AARON used to only sort the success list when not returning detailed responses, check OK
    Collections.sort(successfulInsertions);

    if (!returnDocumentResponses) {
      return nonPerDocumentResult();
    }

    return perDocumentResult();
  }

  /**
   * Returns a insert command result in the newer style of detailed results per document
   *
   * <p>aaron - 3 sept -2024 - code moved from the get() method
   *
   * @return Command result
   */
  private CommandResult perDocumentResult() {
    // New style output: detailed responses.
    InsertionResult[] results = new InsertionResult[allInsertions.size()];
    List<CommandResult.Error> errors = new ArrayList<>();

    // Results array filled in order: first successful insertions
    for (InsertAttempt okInsertion : successfulInsertions) {
      results[okInsertion.position()] =
          new InsertionResult(okInsertion.docRowID().orElseThrow(), InsertionStatus.OK, null);
    }
    // Second: failed insertions; output in order of insertion
    for (InsertAttempt failedInsertion : failedInsertions) {
      // TODO AARON - confirm the null handling in the getError
      CommandResult.Error error = getErrorObject(failedInsertion);

      // We want to avoid adding the same error multiple times, so we keep track of the index:
      // either one exists, use it; or if not, add it and use the new index.
      int errorIdx = errors.indexOf(error);
      if (errorIdx < 0) { // new non-dup error; add it
        errorIdx = errors.size(); // will be appended at the end
        errors.add(error);
      }
      results[failedInsertion.position()] =
          new InsertionResult(
              failedInsertion.docRowID().orElseThrow(), InsertionStatus.ERROR, errorIdx);
    }

    // And third, if any, skipped insertions; those that were not attempted (f.ex due
    // to failure for ordered inserts)
    for (int i = 0; i < results.length; i++) {
      if (null == results[i]) {
        results[i] =
            new InsertionResult(
                allInsertions.get(i).docRowID().orElseThrow(), InsertionStatus.SKIPPED, null);
      }
    }
    Map<CommandStatus, Object> status = new HashMap<>();
    status.put(CommandStatus.DOCUMENT_RESPONSES, Arrays.asList(results));
    maybeAddSchema(status);

    return new CommandResult(null, status, errors);
  }

  /**
   * Returns a insert command result in the original style, without detailed document responses.
   *
   * <p>aaron - 3 sept -2024 - code moved from the get() method
   *
   * @return Command result
   */
  private CommandResult nonPerDocumentResult() {

    List<CommandResult.Error> errors =
        failedInsertions.isEmpty()
            ? null
            : failedInsertions.stream().map(this::getErrorObject).toList();

    // Note: See DocRowIdentifer, it has an attribute that will be called for JSON serialization
    List<DocRowIdentifer> insertedIds =
        successfulInsertions.stream()
            .map(InsertAttempt::docRowID)
            .map(Optional::orElseThrow)
            .toList();

    Map<CommandStatus, Object> status = new HashMap<>();
    status.put(CommandStatus.INSERTED_IDS, insertedIds);
    maybeAddSchema(status);
    return new CommandResult(null, status, errors);
  }

  /**
   * Adds the schema for the first insert attempt to the status map, is the first insert attempt has
   * schema to report.
   *
   * <p>Uses the first, not the first successful, because we may fail to do an insert but will still
   * the _id or PK to report.
   *
   * @param status Map to add the status to
   */
  private void maybeAddSchema(Map<CommandStatus, Object> status) {
    if (allInsertions.isEmpty()) {
      return;
    }
    allInsertions
        .getFirst()
        .schemaDescription()
        .ifPresent(o -> status.put(CommandStatus.PRIMARY_KEY_SCHEMA, o));
  }

  /**
   * Gets the appropriately formatted error given {@link #useErrorObjectV2} and {@link #debugMode}.
   */
  private CommandResult.Error getErrorObject(InsertAttempt insertAttempt) {

    var throwable = insertAttempt.failure().orElse(null);
    if (throwable instanceof APIException apiException) {
      // new v2 error object, with family etc.
      // the builder will handle the debug mode and extended errors settings to return a V1 or V2
      // error
      return apiExceptionToError.apply(apiException);
    }
    if (useErrorObjectV2) {
      return getErrorObjectV2(throwable);
    }

    return getErrorObjectV1(insertAttempt);
  }

  /**
   * Original error object V1, before the family etc, when the exception is not a ApiException aaron
   * - 3 sept 2024 - old code, moved but mostly left alone.
   *
   * <p>the reference "how it used to work" version of this class is the commit at <code>
   * c3ea7b01ef5b658b4bd51be7fc98b0e0333c3e87</code>. In that, when generating the errors for
   * perDocument response it called the mapper without a message prefix, it added the prefix "Failed
   * to insert document with _id %s: %s" when doing the non perDocument responsed. Replicating below
   * for we fail the IT's in {@link InsertIntegrationTest.InsertManyFails} for per doc responses. In
   * this reference version this was the function "getError(Throwable throwable)"
   */
  private CommandResult.Error getErrorObjectV1(InsertAttempt insertAttempt) {

    // aaron we should not be here unless the attempt has failed.
    var throwable =
        insertAttempt
            .failure()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "getErrorObjectV1: attempting to get an error object for an insertAttempt that has not failed. insertAttempt: %s",
                            insertAttempt)));

    if (returnDocumentResponses) {
      return ThrowableToErrorMapper.getMapperWithMessageFunction()
          .apply(throwable, throwable.getMessage());
    }

    String message =
        "Failed to insert document with _id %s: %s"
            .formatted(insertAttempt.docRowID().orElseThrow(), throwable.getMessage());
    /// TODO: confirm the null handling in the getMapperWithMessageFunction
    // passing null is what would have happened before changing to optional
    // BUG: this does not handle is the debug flag is set.
    return ThrowableToErrorMapper.getMapperWithMessageFunction()
        .apply(insertAttempt.failure().orElse(null), message);
  }

  /**
   * aaron - I think this generating the V2 messages, but it does not look like it. copied from what
   * was here and left alone
   */
  private static CommandResult.Error getErrorObjectV2(Throwable throwable) {
    // TODO AARON - confirm we have two different error message paths
    return ThrowableToErrorMapper.getMapperWithMessageFunction()
        .apply(throwable, throwable.getMessage());
  }

  /**
   * Aggregates the result of the insert operation into this object, used when building the page
   * from running inserts.
   *
   * @param insertion Document insertion attempt
   */
  public void registerCompletedAttempt(InsertAttempt insertion) {
    // TODO: AARON: confirm this should not add to the allInsertions list. It would seem better if
    // it did
    insertion
        .failure()
        .ifPresentOrElse(
            throwable -> failedInsertions.add(insertion),
            () -> successfulInsertions.add(insertion));
  }
}
