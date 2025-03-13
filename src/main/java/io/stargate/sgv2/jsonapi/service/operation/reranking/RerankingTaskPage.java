package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.CompositeTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskPage;
import java.util.Collection;
import java.util.function.Supplier;

/** */
public class RerankingTaskPage<SchemaT extends TableBasedSchemaObject>
    extends TaskPage<RerankingTask<SchemaT>, SchemaT> {

  private RerankingTaskPage(
      TaskGroup<RerankingTask<SchemaT>, SchemaT> tasks, CommandResultBuilder resultBuilder) {
    super(tasks, resultBuilder);
  }

  public static <SchemaT extends TableBasedSchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {

    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {
    // add any errors and warnings
    super.buildCommandResult();

    // Get the documents from each task and add them to the result
    // TODO: there will only be 1 task, check this ?
    tasks.completedTasks().stream()
        .map(RerankingTask::rerankingTaskResult)
        .map(RerankingTask.RerankingTaskResult::rerankedDocuments)
        .flatMap(Collection::stream)
        .forEach(resultBuilder::addDocument);
  }

  /**
   * Accumulates the completed {@link CompositeTask}s so the final result can be built from the last
   * task.
   *
   * @param <SchemaT>
   */
  public static class Accumulator<SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<RerankingTask<SchemaT>, SchemaT> {

    protected Accumulator() {}

    @Override
    public Supplier<CommandResult> getResults() {

      return new RerankingTaskPage<>(
          tasks, CommandResult.multiDocumentBuilder(useErrorObjectV2, debugMode, requestTracing));
    }
  }
}
