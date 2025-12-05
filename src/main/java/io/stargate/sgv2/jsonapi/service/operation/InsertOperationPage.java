package io.stargate.sgv2.jsonapi.service.operation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * IMPORTANT: THIS IS ALSO USED BY THE COLLECTIONS (JUST FOR INSERT) SO IT NEEDS TO STAY UNTIL
 * COLLECTIONS CODE IS UPDATED (INSERTS STARTED THE "ATTEMPT" PATTERN)
 *
 * <p>Builds the response for an insert operation or one or more {@link InsertAttempt}s.
 *
 * <p>Keeps track of the inserts, their success status, and then builds the {@link CommandResult}
 * via {@link #get()}.
 *
 * <p>Create an instance with all the insert attempts the insert operation will process, then call
 * {@link #registerCompletedAttempt} for each completed attempt, the instance will then track failed
 * and successful attempts. This is used as an aggregator for {@link
 * io.smallrye.mutiny.groups.MultiCollect#in(Supplier, BiConsumer)}
 */
public class InsertOperationPage<SchemaT extends TableBasedSchemaObject>
    implements Supplier<CommandResult> {

  // All the insertions that are going to be attempted
  private final List<InsertAttempt<SchemaT>> allInsertions;

  // True if the response should include detailed info for each document
  private final boolean returnDocumentResponses;

  // The success and failed lists are mutable and are used to build the response
  private final List<InsertAttempt<SchemaT>> successfulInsertions;
  private final List<InsertAttempt<SchemaT>> failedInsertions;

  // Flagged true to include the new error object v2
  private final boolean useErrorObjectV2;

  private final RequestTracing requestTracing;

  // Created in the Ctor
  private final APIExceptionCommandErrorBuilder apiExceptionToError;

  /** Create an instance that has debug false and useErrorObjectV2 false */
  public InsertOperationPage(
      List<? extends InsertAttempt<SchemaT>> allAttemptedInsertions,
      boolean returnDocumentResponses) {
    this(allAttemptedInsertions, returnDocumentResponses, false, RequestTracing.NO_OP);
  }

  /**
   * Create an instance with the given parameters
   *
   * @param allAttemptedInsertions All the insertions that are going to be attempted. Accepts
   *     wildcard to allow for implementations of the {@link InsertAttempt} interface to be passed
   *     easily.
   * @param returnDocumentResponses If the response should include detailed info for each document.
   * @param useErrorObjectV2 Flagged true to include the new error object v2.
   */
  public InsertOperationPage(
      List<? extends InsertAttempt<SchemaT>> allAttemptedInsertions,
      boolean returnDocumentResponses,
      boolean useErrorObjectV2,
      RequestTracing requestTracing) {

    Objects.requireNonNull(allAttemptedInsertions, "allAttemptedInsertions cannot be null");
    this.allInsertions =
        allAttemptedInsertions.stream().map(attempt -> (InsertAttempt<SchemaT>) attempt).toList();
    this.returnDocumentResponses = returnDocumentResponses;

    this.successfulInsertions = new ArrayList<>(allAttemptedInsertions.size());
    this.failedInsertions = new ArrayList<>(allAttemptedInsertions.size());
    this.useErrorObjectV2 = useErrorObjectV2;
    this.requestTracing = requestTracing;
    this.apiExceptionToError = new APIExceptionCommandErrorBuilder(useErrorObjectV2);
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

    var builder = CommandResult.statusOnlyBuilder(useErrorObjectV2, requestTracing);
    return returnDocumentResponses ? perDocumentResult(builder) : nonPerDocumentResult(builder);
  }

  /**
   * Returns an insert command result in the newer style of detailed results per document
   *
   * <p>aaron - 3 sept -2024 - code moved from the get() method
   *
   * @return Command result
   */
  private CommandResult perDocumentResult(CommandResultBuilder builder) {
    // New style output: detailed responses.
    InsertionResult[] results = new InsertionResult[allInsertions.size()];
    List<CommandResult.Error> errors = new ArrayList<>();

    // Results array filled in order: first successful insertions
    for (var okInsertion : successfulInsertions) {
      results[okInsertion.position()] =
          new InsertionResult(okInsertion.docRowID().orElseThrow(), InsertionStatus.OK, null);
    }

    // Second: failed insertions; output in order of insertion
    for (var failedInsertion : failedInsertions) {
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
    builder.addStatus(CommandStatus.DOCUMENT_RESPONSES, Arrays.asList(results));
    builder.addCommandResultError(errors);
    maybeAddSchema(builder);

    return builder.build();
  }

  /**
   * Returns an insert command result in the original style, without detailed document responses.
   *
   * <p>aaron - 3 sept -2024 - code moved from the get() method
   *
   * @return Command result
   */
  private CommandResult nonPerDocumentResult(CommandResultBuilder builder) {

    failedInsertions.stream().map(this::getErrorObject).forEach(builder::addCommandResultError);

    // Note: See DocRowIdentifer, it has an attribute that will be called for JSON serialization
    List<DocRowIdentifer> insertedIds =
        successfulInsertions.stream()
            .map(InsertAttempt::docRowID)
            .map(Optional::orElseThrow)
            .toList();

    builder.addStatus(CommandStatus.INSERTED_IDS, insertedIds);
    maybeAddSchema(builder);

    return builder.build();
  }

  /**
   * Adds the schema for the first insert attempt to the status map, if the first insert attempt has
   * schema to report.
   *
   * <p>Uses the first, not the first successful, because we may fail to do an insert but will still
   * have the _id or PK to report.
   *
   * @param builder CommandResult builder to add the status to
   */
  private void maybeAddSchema(CommandResultBuilder builder) {
    if (allInsertions.isEmpty()) {
      return;
    }
    allInsertions
        .getFirst()
        .schemaDescription()
        .ifPresent(o -> builder.addStatus(CommandStatus.PRIMARY_KEY_SCHEMA, o));
  }

  /** Gets the appropriately formatted error given {@link #useErrorObjectV2}. */
  private CommandResult.Error getErrorObject(InsertAttempt<SchemaT> insertAttempt) {

    var throwable = insertAttempt.failure().orElse(null);
    if (throwable instanceof APIException apiException) {
      // new v2 error object, with family etc.
      // the builder will handle the debug mode and extended errors settings to return a V1 or V2
      // error
      return apiExceptionToError.buildLegacyCommandResultError(apiException);
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
  private CommandResult.Error getErrorObjectV1(InsertAttempt<SchemaT> insertAttempt) {

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

    // aaron 23 sept - the insertAttempt.docRowID().orElse.. below is because if we fail to shred we
    // do not have the id
    // previously this type of error would bubble to the top of the stack, it is not handled as part
    // of building the
    // insert page. This is ugly, need to fix later.
    var docRowID = insertAttempt.docRowID().orElse(() -> "UNKNOWN").value();

    String message =
        allInsertions.size() == 1
            ? throwable.getMessage()
            : "Failed to insert document with _id " + docRowID + ": " + throwable.getMessage();

    /// TODO: confirm the null handling in the getMapperWithMessageFunction
    // passing null is what would have happened before changing to optional
    // BUG: this does not handle if the debug flag is set.
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
  public void registerCompletedAttempt(InsertAttempt<SchemaT> insertion) {
    // TODO: AARON: confirm this should not add to the allInsertions list. It would seem better if
    // it did
    insertion
        .failure()
        .ifPresentOrElse(
            throwable -> failedInsertions.add(insertion),
            () -> successfulInsertions.add(insertion));
  }
}
