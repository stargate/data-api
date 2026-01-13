package io.stargate.sgv2.jsonapi.service.operation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowId;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A page of results from an insert command, use {@link #accumulator(CommandContext)} to get a
 * builder to pass to {@link GenericOperation}.
 *
 * <p><b>NOTE</b> a lot of this duplicates {@link InsertOperationPage}, that class will eventually
 * be replaced by this one.
 */
public class InsertDBTaskPage<SchemaT extends TableBasedSchemaObject>
    extends DBTaskPage<InsertDBTask<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InsertDBTaskPage.class);

  // True if the response should include detailed info for each document
  private final boolean returnDocumentResponses;

  private static final CommandErrorFactory commandErrorFactory = new CommandErrorFactory();

  private InsertDBTaskPage(
      TaskGroup<InsertDBTask<SchemaT>, SchemaT> tasks,
      CommandResultBuilder resultBuilder,
      boolean returnDocumentResponses) {
    super(tasks, resultBuilder);
    this.returnDocumentResponses = returnDocumentResponses;
  }

  public static <SchemaT extends TableBasedSchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {

    // Do not call the super buildCommandResult() because there are different ways we add the errors
    addTaskWarningsToResult();

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

    addTaskErrorsToResult();

    // Note: See DocRowIdentifer, it has an attribute that will be called for JSON serialization
    List<DocRowIdentifer> insertedIds =
        tasks.completedTasks().stream()
            .map(InsertDBTask::docRowID)
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
      maybeAddSchema(CommandStatus.PRIMARY_KEY_SCHEMA);
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

    var results = new InsertionResult[tasks.size()];

    // Results array filled in order: first successful insertions
    for (var task : tasks.completedTasks()) {
      results[task.position()] =
          new InsertionResult(task.docRowID().orElseThrow(), InsertionStatus.OK, null);
    }

    List<CommandErrorV2> seenErrors = new ArrayList<>();
    // Second: failed insertions; output in order of insertion
    for (var task : tasks.errorTasks()) {
      // XXX - AARON- TODO: the task erorr is throwable, so mapping to Error will fail for regular
      // runtime errorTasks
      var cmdError = commandErrorFactory.create(task.failure().orElseThrow());

      // We want to avoid adding the same error multiple times, so we keep track of the index:
      // either one exists, use it; or if not, add it and use the new index.
      int errorIdx = indexOf(seenErrors, cmdError);
      if (errorIdx < 0) { // new non-dup error; add it
        errorIdx = seenErrors.size(); // will be appended at the end
        seenErrors.add(cmdError);
      }
      results[task.position()] =
          new InsertionResult(
              task.docRowID().orElse(RowId.EMPTY_ROWID), InsertionStatus.ERROR, errorIdx);
    }

    // And third, if any, skipped insertions; those that were not attempted (f.ex due
    // to failure for ordered inserts)
    for (var task : tasks.skippedTasks()) {
      results[task.position()] =
          new InsertionResult(task.docRowID().orElseThrow(), InsertionStatus.SKIPPED, null);
    }

    resultBuilder.addCommandError(seenErrors);
    resultBuilder.addStatus(CommandStatus.DOCUMENT_RESPONSES, Arrays.asList(results));
    maybeAddSchema(CommandStatus.PRIMARY_KEY_SCHEMA);
  }

  /**
   * Custom indexOf method that ignores the id field when used with Error Object v2 because it is
   * different for every error.
   */
  private int indexOf(List<CommandErrorV2> seenErrors, CommandErrorV2 searchError) {

    var predicate = searchError.nonIdentityMatcher();
    for (int i = 0; i < seenErrors.size(); i++) {
      if (predicate.test(seenErrors.get(i))) {
        return i;
      }
    }
    return -1;
  }

  enum InsertionStatus {
    OK,
    ERROR,
    SKIPPED
  }

  @JsonPropertyOrder({"_id", "status", "errorsIdx"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  record InsertionResult(DocRowIdentifer _id, InsertionStatus status, Integer errorsIdx) {}

  public static class Accumulator<SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<InsertDBTask<SchemaT>, SchemaT> {

    private boolean returnDocumentResponses = false;

    Accumulator() {}

    public Accumulator<SchemaT> returnDocumentResponses(boolean returnDocumentResponses) {
      this.returnDocumentResponses = returnDocumentResponses;
      return this;
    }

    @Override
    public InsertDBTaskPage<SchemaT> getResults() {

      return new InsertDBTaskPage<>(
          tasks, CommandResult.statusOnlyBuilder(requestTracing), returnDocumentResponses);
    }
  }
}
