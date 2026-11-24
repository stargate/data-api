package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskGroupBuilder;
import io.stargate.sgv2.jsonapi.service.operation.reranking.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the operation for a {@link FindAndRerankCommand} from the {@link FindAndRerankPlan} made
 * by {@link FindAndRerankPlanner}, and from the command for the filter and for the hybridLimits
 * hack in readTasks() (GH #1961). The operation runs one read for each {@link
 * FindAndRerankPlan.ReadPlan} of the plan, then reranks the documents from all the reads.
 *
 * <p>The builder trusts the plan is valid, so it does not check the plan as it reads from it.
 *
 * <p>TODO: move the filter and the hybridLimits into the plan, so the builder only needs the plan.
 */
class FindAndRerankOperationBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FindAndRerankOperationBuilder.class);

  // Need a Projection for the inner finds, it's too complicated to merge the user projection
  // and what we need, because the user may be running a projection to hide fields, which
  // cannot then include fields, so we just use the * wildcard to include all fields for now
  private static final JsonNode INCLUDE_ALL_PROJECTION =
      JsonNodeFactory.instance.objectNode().put("*", 1);

  private final CommandContext<CollectionSchemaObject> commandContext;

  // things set in the builder pattern.
  private FindAndRerankCommand command;
  private FindAndRerankPlan plan;
  private FindCommandResolver findCommandResolver;

  public FindAndRerankOperationBuilder(CommandContext<CollectionSchemaObject> commandContext) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");
  }

  public FindAndRerankOperationBuilder withCommand(FindAndRerankCommand command) {
    this.command = command;
    return this;
  }

  public FindAndRerankOperationBuilder withPlan(FindAndRerankPlan plan) {
    this.plan = plan;
    return this;
  }

  public FindAndRerankOperationBuilder withFindCommandResolver(
      FindCommandResolver findCommandResolver) {
    this.findCommandResolver = findCommandResolver;
    return this;
  }

  public Operation<CollectionSchemaObject> build() {

    Objects.requireNonNull(command, "command cannot be null");
    Objects.requireNonNull(plan, "plan cannot be null");

    // Step 1 - we need a reranking task and the deferrable actions to do the intermediate reads
    // Each read of the plan gets a deferred result, the read fills it and the reranking task waits
    // for it
    var readsAndResults =
        plan.reads().stream()
            .map(read -> new ReadAndResult(read, new DeferredCommandResult()))
            .toList();

    var rerankTasksAndDeferrables = rerankTasks(readsAndResults);

    // Step 2 - we need to read the data from the collections, we are wrapping the old collections
    // in the new tasks so we do not change the collection code
    var readTasksAndDeferrables = readTasks(readsAndResults);

    // Step 3 - we may need an embedding task, lets get one of those :)
    var embeddingActions =
        DeferredAction.filtered(
            EmbeddingDeferredAction.class,
            Deferrable.deferred(readTasksAndDeferrables.deferrables()));
    var embeddingTaskGroup =
        embeddingActions.isEmpty()
            ? null
            : new EmbeddingTaskGroupBuilder<CollectionSchemaObject>()
                .withCommandContext(commandContext)
                .withRequestType(EmbeddingProvider.EmbeddingRequestType.SEARCH)
                .withEmbeddingActions(embeddingActions)
                .build();

    // Step 4 - build the composite tasks and wrap them in an operation
    // we had to build from the last to the first steps, now add them in the order we want them to
    // run, we will only have an embedding task if we needed to do a vectorize
    var compositeBuilder = new CompositeTaskOperationBuilder<>(commandContext);
    if (embeddingTaskGroup != null) {
      compositeBuilder.withIntermediateTasks(embeddingTaskGroup, TaskRetryPolicy.NO_RETRY);
    }
    compositeBuilder.withIntermediateTasks(
        readTasksAndDeferrables.taskGroup(), TaskRetryPolicy.NO_RETRY);

    return compositeBuilder.build(
        rerankTasksAndDeferrables.taskGroup(),
        TaskRetryPolicy.NO_RETRY,
        rerankTasksAndDeferrables.accumulator());
  }

  private TaskGroupAndDeferrables<RerankingTask<CollectionSchemaObject>, CollectionSchemaObject>
      rerankTasks(List<ReadAndResult> readsAndResults) {

    var rerankStep = plan.rerankStep();
    var rerankServiceDef = rerankStep.rerankServiceDef();
    RerankingProvider rerankingProvider =
        commandContext
            .rerankingProviderFactory()
            .create(
                commandContext.requestContext().tenant(),
                commandContext.requestContext().authToken(),
                rerankServiceDef.provider(),
                rerankServiceDef.modelName(),
                rerankServiceDef.authentication(),
                commandContext.commandName());

    // the reranking task needs to know if each result is from a vector or a BM25 read
    var resultsWithSource =
        readsAndResults.stream()
            .map(
                readAndResult ->
                    new RerankingTask.DeferredCommandWithSource(
                        readAndResult.readPlan().rankSource(),
                        readAndResult.deferredCommandResult()))
            .toList();

    // todo: move to a builder pattern, mostly to make it easier to manage the task position and
    // retry policy
    RerankingTask<CollectionSchemaObject> task =
        new RerankingTask<>(
            0,
            commandContext.schemaObject(),
            TaskRetryPolicy.NO_RETRY,
            rerankingProvider,
            rerankStep.rerankingQuery(),
            rerankStep.passageLocator(),
            rerankStep.projector(),
            resultsWithSource,
            rerankStep.limit());

    // there is only 1 task, but making it clear that we want sequential for this step
    TaskGroup<RerankingTask<CollectionSchemaObject>, CollectionSchemaObject> taskGroup =
        new TaskGroup<>(true);
    taskGroup.add(task);

    var rerankAccumulator =
        RerankingTaskPage.accumulator(commandContext)
            .withIncludeScores(plan.includeScores())
            .withIncludeSortVector(plan.includeSortVector());

    return new TaskGroupAndDeferrables<>(
        taskGroup,
        rerankAccumulator,
        resultsWithSource.stream()
            .map(RerankingTask.DeferredCommandWithSource::deferredRead)
            .collect(java.util.stream.Collectors.toUnmodifiableList()));
  }

  private TaskGroupAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject> readTasks(
      List<ReadAndResult> readsAndResults) {

    // we can run these tasks in parallel
    TaskGroup<IntermediateCollectionReadTask, CollectionSchemaObject> taskGroup =
        new TaskGroup<>(false);

    // Hack: See https://github.com/stargate/data-api/issues/1961
    // copying the hybrid limits on the command context so the find command resolver can pick it up
    // when the command runs later, so we can set the page size to be the same as the limit
    // A sort without $hybrid has no hybridLimits, so we copy the read limit of its one read
    commandContext.setHybridLimits(
        switch (command.sortClause()) {
          case FindAndRerankSort.MultiLeg ignored ->
              getOrDefault(
                  command.options(),
                  FindAndRerankCommand.Options::hybridLimits,
                  FindAndRerankCommand.HybridLimits.DEFAULT);
          case FindAndRerankSort.SingleLeg ignored -> {
            var readLimit = plan.reads().getFirst().readLimit();
            yield new FindAndRerankCommand.HybridLimits(
                readLimit, readLimit, CommandFeatures.EMPTY);
          }
        });

    var deferrables = new ArrayList<Deferrable>();

    // one task for each read, the task fills the deferred result of its read
    for (int position = 0; position < readsAndResults.size(); position++) {
      var readAndResult = readsAndResults.get(position);

      // the read calls this action when it is done, to fill the deferredCommandResult
      var deferredCommandResultAction =
          DeferredAction.filtered(
                  DeferredCommandResultAction.class,
                  Deferrable.deferred(readAndResult.deferredCommandResult()))
              .getFirst();

      var taskAndDeferrables =
          switch (readAndResult.readPlan()) {
            case FindAndRerankPlan.VectorReadPlan(var vector, var readLimit) ->
                buildVectorRead(position, vector, readLimit, deferredCommandResultAction);
            case FindAndRerankPlan.VectorizeReadPlan(var readLimit) ->
                buildVectorizeRead(position, readLimit, deferredCommandResultAction);
            case FindAndRerankPlan.LexicalReadPlan(var lexicalText, var readLimit) ->
                buildBm25Read(position, lexicalText, readLimit, deferredCommandResultAction);
          };
      taskGroup.add(taskAndDeferrables.task());
      deferrables.addAll(taskAndDeferrables.deferrables());
    }

    // No accumulator, this will be wrapped in an intermediate composite task
    return new TaskGroupAndDeferrables<>(taskGroup, null, deferrables);
  }

  private TaskAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject> buildBm25Read(
      int position, String lexicalText, int readLimit, DeferredCommandResultAction deferredAction) {

    var bm25SortClause = new SortClause(List.of(SortExpression.collectionLexicalSort(lexicalText)));
    return new TaskAndDeferrables<>(
        buildReadTask(position, bm25SortClause, null, readLimit, deferredAction));
  }

  /** Builds a read that sorts with the vector from the command. */
  private TaskAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject>
      buildVectorRead(
          int position, float[] vector, int readLimit, DeferredCommandResultAction deferredAction) {

    var vectorSortClause = new SortClause(List.of(SortExpression.collectionVectorSort(vector)));
    return new TaskAndDeferrables<>(
        buildReadTask(position, vectorSortClause, null, readLimit, deferredAction));
  }

  /** Builds a read that sorts with the vector from the vectorize step of the plan. */
  private TaskAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject>
      buildVectorizeRead(int position, int readLimit, DeferredCommandResultAction deferredAction) {

    // the sort clause is empty, the deferred vectorize adds the vector sort to it when it gets the
    // vector, so the sort clause must be mutable
    var vectorSortClause = new SortClause(new ArrayList<>());
    var vectorizeStep = plan.vectorizeStep();
    var deferredVectorize =
        new DeferredVectorize(
            vectorizeStep.text(),
            vectorizeStep.dimension(),
            vectorizeStep.vectorizeDefinition(),
            vectorSortClause);

    return new TaskAndDeferrables<>(
        buildReadTask(position, vectorSortClause, deferredVectorize, readLimit, deferredAction),
        deferredVectorize);
  }

  /**
   * Builds the task for one read: an inner find with the filter of the command and the sort.
   *
   * @param position The position of the task in its task group.
   * @param sortClause The sort of the inner find.
   * @param deferredVectorize The vectorize the task waits for before it reads, <code>null</code>
   *     when the sort clause already has the sort.
   * @param readLimit The limit of the inner find.
   * @param deferredAction The action the task calls with the result of the inner find.
   */
  private IntermediateCollectionReadTask buildReadTask(
      int position,
      SortClause sortClause,
      DeferredVectorize deferredVectorize,
      int readLimit,
      DeferredCommandResultAction deferredAction) {

    var readCommand =
        new FindCommand(
            command.filterDefinition(),
            INCLUDE_ALL_PROJECTION,
            SortDefinition.wrap(sortClause),
            new FindCommand.Options(
                readLimit, 0, null, plan.includeScores(), plan.includeSortVector()));

    return new IntermediateCollectionReadTask(
        position,
        commandContext.schemaObject(),
        TaskRetryPolicy.NO_RETRY,
        findCommandResolver,
        readCommand,
        deferredVectorize,
        deferredAction);
  }

  /**
   * A read of the plan and its deferred result.
   *
   * <p>The task that readTasks() builds for the read fills the result, and the reranking task that
   * rerankTasks() builds waits for it.
   *
   * @param readPlan The read.
   * @param deferredCommandResult The deferred result that the task for the read fills.
   */
  private record ReadAndResult(
      FindAndRerankPlan.ReadPlan readPlan, DeferredCommandResult deferredCommandResult) {

    ReadAndResult {
      Objects.requireNonNull(readPlan, "readPlan must not be null");
      Objects.requireNonNull(deferredCommandResult, "deferredCommandResult must not be null");
    }
  }
}
