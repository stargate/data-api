package io.stargate.sgv2.jsonapi.service.operation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import java.util.*;

/**
 * A page of results from an insert command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 *
 * <p><b>NOTE</b> a lot of this duplicates {@link InsertOperationPage}, that class will eventually
 * be replaced by this one.
 */
public class InsertAttemptPage<SchemaT extends TableBasedSchemaObject>
    extends OperationAttemptPage<SchemaT, InsertAttempt<SchemaT>> {

  // True if the response should include detailed info for each document
  private final boolean returnDocumentResponses;

  private InsertAttemptPage(
      OperationAttemptContainer<SchemaT, InsertAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder,
      boolean returnDocumentResponses) {
    super(attempts, resultBuilder);
    this.returnDocumentResponses = returnDocumentResponses;
  }

  public static <SchemaT extends TableBasedSchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
  }

  @Override
  protected void buildCommandResult() {

    // Do not call the super buildCommandResult() because there are different ways we add the errors
    addAttemptWarningsToResult();

    if (returnDocumentResponses) {
      buildPerDocumentResult();
    } else {
      buildNonPerDocumentResult();
    }
  }

  /**
   * Returns a insert command result in the original style, without detailed document responses.
   *
   * <p>aaron - 3 sept -2024 - code moved from the get() method
   *
   * @return Command result
   */
  private void buildNonPerDocumentResult() {

    addAttemptErrorsToResult();

    // Note: See DocRowIdentifer, it has an attribute that will be called for JSON serialization
    List<DocRowIdentifer> insertedIds =
        attempts.completedAttempts().stream()
            .map(InsertAttempt::docRowID)
            .map(Optional::orElseThrow)
            .toList();

    // Only include the inserted ids if there was any, this is slightly diff to original
    // InsertOperationPage that would only add if there were no errors, should be same though
    if (!insertedIds.isEmpty()) {
      resultBuilder.addStatus(CommandStatus.INSERTED_IDS, insertedIds);

      // for the non document response, only add schema if we have some inserted Id's because if we
      // do not
      // the insertIds status will not be included. When we have per doc responses, we add the
      // schema in all cases.
      maybeAddSchema();
    }
  }

  /**
   * Returns a insert command result in the newer style of detailed results per document
   *
   * <p>aaron - 3 sept -2024 - code moved from the get() method
   *
   * @return Command result
   */
  private void buildPerDocumentResult() {
    // aaron - 26 sept - this could be re-written now to simply iterate the attempts in loop and
    // check their status,
    // kept using the same approach as InsertOperationPage to make comparison easy until we remove
    // the old class

    var results = new InsertionResult[attempts.size()];

    // Results array filled in order: first successful insertions
    for (var attempt : attempts.completedAttempts()) {
      results[attempt.position()] =
          new InsertionResult(attempt.docRowID().orElseThrow(), InsertionStatus.OK, null);
    }

    List<CommandResult.Error> seenErrors = new ArrayList<>();
    // Second: failed insertions; output in order of insertion
    for (var attempt : attempts.errorAttempts()) {
      var cmdError = resultBuilder.throwableToCommandError(attempt.failure().orElseThrow());

      // We want to avoid adding the same error multiple times, so we keep track of the index:
      // either one exists, use it; or if not, add it and use the new index.
      int errorIdx = indexOf(seenErrors, cmdError);
      if (errorIdx < 0) { // new non-dup error; add it
        errorIdx = seenErrors.size(); // will be appended at the end
        seenErrors.add(cmdError);
      }
      results[attempt.position()] =
          new InsertionResult(attempt.docRowID().orElseThrow(), InsertionStatus.ERROR, errorIdx);
    }

    // And third, if any, skipped insertions; those that were not attempted (f.ex due
    // to failure for ordered inserts)
    for (var attempt : attempts.skippedAttempts()) {
      results[attempt.position()] =
          new InsertionResult(attempt.docRowID().orElseThrow(), InsertionStatus.SKIPPED, null);
    }

    seenErrors.forEach(resultBuilder::addCommandResultError);
    resultBuilder.addStatus(CommandStatus.DOCUMENT_RESPONSES, Arrays.asList(results));
    maybeAddSchema();
  }

  /**
   * Custom indexOf method that ignores the id field when used with Error Object v2 because it is
   * different for every error.
   */
  private int indexOf(List<CommandResult.Error> seenErrors, CommandResult.Error searchError) {

    for (int i = 0; i < seenErrors.size(); i++) {
      var seenError = seenErrors.get(i);
      var fields1 = new HashMap<>(seenError.fields());
      var fields2 = new HashMap<>(searchError.fields());
      fields1.remove(ErrorObjectV2Constants.Fields.ID);
      fields2.remove(ErrorObjectV2Constants.Fields.ID);
      fields1.put(ErrorObjectV2Constants.Fields.MESSAGE, seenError.message());
      fields2.put(ErrorObjectV2Constants.Fields.MESSAGE, searchError.message());
      if (fields1.equals(fields2)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Adds the schema for the first insert attempt to the status map, if the first insert attempt has
   * schema to report.
   *
   * <p>Uses the first, not the first successful, because we may fail to do an insert but will still
   * have the _id or PK to report.
   */
  private void maybeAddSchema() {
    if (attempts.isEmpty()) {
      return;
    }

    attempts
        .getFirst()
        .schemaDescription()
        .ifPresent(object -> resultBuilder.addStatus(CommandStatus.PRIMARY_KEY_SCHEMA, object));
  }

  enum InsertionStatus {
    OK,
    ERROR,
    SKIPPED
  }

  @JsonPropertyOrder({"_id", "status", "errorsIdx"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  record InsertionResult(DocRowIdentifer _id, InsertionStatus status, Integer errorsIdx) {}

  public static class Builder<SchemaT extends TableBasedSchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, InsertAttempt<SchemaT>> {

    private boolean returnDocumentResponses = false;

    Builder() {}

    public Builder<SchemaT> returnDocumentResponses(boolean returnDocumentResponses) {
      this.returnDocumentResponses = returnDocumentResponses;
      return this;
    }

    @Override
    public InsertAttemptPage<SchemaT> getOperationPage() {

      return new InsertAttemptPage<>(
          attempts,
          CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode),
          returnDocumentResponses);
    }
  }
}
