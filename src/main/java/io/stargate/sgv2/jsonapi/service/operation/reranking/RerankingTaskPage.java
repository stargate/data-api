package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.operation.tasks.CompositeTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskPage;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** */
public class RerankingTaskPage<SchemaT extends TableBasedSchemaObject>
    extends TaskPage<RerankingTask<SchemaT>, SchemaT> {

  private boolean includeScores;
  private boolean includeSortVector;

  private RerankingTaskPage(
      TaskGroup<RerankingTask<SchemaT>, SchemaT> tasks,
      CommandResultBuilder resultBuilder,
      boolean includeScores,
      boolean includeSortVector) {
    super(tasks, resultBuilder);

    this.includeScores = includeScores;
    this.includeSortVector = includeSortVector;
  }

  public static <SchemaT extends TableBasedSchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {

    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {

    // There should only be 1 rerankig task
    if (tasks.completedTasks().size() != 1) {
      throw new IllegalStateException(
          "Expected exactly 1 completed RerankingTask, got " + tasks.completedTasks().size());
    }
    var completedTask = tasks.completedTasks().getFirst();

    // add any errors and warnings
    super.buildCommandResult();

    if (includeSortVector) {
      // to match with the find commands, we include the status field and it may be null
      resultBuilder.addStatus(CommandStatus.SORT_VECTOR, completedTask.sortVector());
    }

    var rerankedDocuments = completedTask.rerankingTaskResult().rerankedDocuments();
    List<DocumentScoresDesc> scores =
        includeScores ? new ArrayList<>(rerankedDocuments.size()) : null;
    // These should be the reranked documents in order
    rerankedDocuments.stream()
        .forEachOrdered(
            scoredDocument -> {
              resultBuilder.addDocument(scoredDocument.document());
              if (scores != null) {
                scores.add(scoredDocument.scores().scoresDesc());
              }
            });

    if (scores != null) {
      resultBuilder.addStatus(CommandStatus.DOCUMENT_RESPONSES, scores);
    }
  }

  /**
   * Accumulates the completed {@link CompositeTask}s so the final result can be built from the last
   * task.
   *
   * @param <SchemaT>
   */
  public static class Accumulator<SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<RerankingTask<SchemaT>, SchemaT> {

    private boolean includeScores = false;
    private boolean includeSortVector = false;

    protected Accumulator() {}

    public Accumulator<SchemaT> withIncludeSortVector(boolean includeSortVector) {
      this.includeSortVector = includeSortVector;
      return this;
    }

    public Accumulator<SchemaT> withIncludeScores(boolean includeScores) {
      this.includeScores = includeScores;
      return this;
    }

    @Override
    public Supplier<CommandResult> getResults() {

      return new RerankingTaskPage<>(
          tasks,
          CommandResult.multiDocumentBuilder(useErrorObjectV2, requestTracing),
          includeScores,
          includeSortVector);
    }
  }
}
