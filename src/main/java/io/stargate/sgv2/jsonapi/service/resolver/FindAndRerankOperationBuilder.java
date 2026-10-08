package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
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
 * Builds the operation for a {@link FindAndRerankCommand}.
 *
 * <p>The {@link FindAndRerankPlanner} validates the command and decides what to run, this builder
 * only turns the {@link FindAndRerankPlan} into tasks: one read task per leg, an embedding task for
 * the legs that need vectorizing, and one reranking task over the results of all the reads.
 */
class FindAndRerankOperationBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FindAndRerankOperationBuilder.class);

  private final CommandContext<CollectionSchemaObject> commandContext;

  // the planner needs it for the hybridLimits bounds and the default limit
  private final OperationsConfig operationsConfig;

  // things set in the builder pattern.
  private FindAndRerankCommand command;
  private FindCommandResolver findCommandResolver;

  public FindAndRerankOperationBuilder(CommandContext<CollectionSchemaObject> commandContext) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");

    operationsConfig = commandContext.config().get(OperationsConfig.class);
  }

  public FindAndRerankOperationBuilder withCommand(FindAndRerankCommand command) {
    this.command = command;
    return this;
  }

  public FindAndRerankOperationBuilder withFindCommandResolver(
      FindCommandResolver findCommandResolver) {
    this.findCommandResolver = findCommandResolver;
    return this;
  }

  public Operation<CollectionSchemaObject> build() {

    Objects.requireNonNull(command, "command cannot be null");

    // the planner validates the request and decides what to run, it throws if the request cannot
    // run. The legMode and explicitLexical come from the parsed sort, not the command features.
    var sort = command.sortClause();
    var plan =
        FindAndRerankPlanner.plan(
            sort.legMode(),
            sort.explicitLexical(),
            sort,
            command.options(),
            commandContext.schemaObject(),
            operationsConfig,
            commandContext.rerankingProviderFactory().getRerankingConfig());

    // Step 1 - we need a reranking task and the deferrable actions to do the intermediate reads
    // Making one deferrable per leg here, in the plan order, so we can associate them with the
    // read that will fill them
    List<RerankingTask.DeferredCommandWithSource> deferredReads =
        plan.legs().stream()
            .map(
                leg ->
                    new RerankingTask.DeferredCommandWithSource(
                        leg.rankSource(), new DeferredCommandResult()))
            .toList();

    var rerankTasksAndDeferrables = rerankTasks(plan, deferredReads);

    // Step 2 - we need to read the data from the collections, we are wrapping the old collections
    // in the new tasks so we do not change the collection code
    var readTasksAndDeferrables = readTasks(plan, deferredReads);

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
      rerankTasks(
          FindAndRerankPlan plan,
          List<RerankingTask.DeferredCommandWithSource> deferredCommandResults) {

    var rerankServiceDef = plan.rerankServiceDef();
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

    // todo: move to a builder pattern, mostly to make it easier to manage the task position and
    // retry policy
    RerankingTask<CollectionSchemaObject> task =
        new RerankingTask<>(
            0,
            commandContext.schemaObject(),
            TaskRetryPolicy.NO_RETRY,
            rerankingProvider,
            plan.rerankingQuery(),
            plan.passageLocator(),
            command.buildProjector(),
            deferredCommandResults,
            plan.limit());

    // there is only 1 task, but making it clear that we want sequential for this step
    TaskGroup<RerankingTask<CollectionSchemaObject>, CollectionSchemaObject> taskGroup =
        new TaskGroup<>(true);
    taskGroup.add(task);

    var rerankAccumulator =
        RerankingTaskPage.accumulator(commandContext)
            .withIncludeScores(
                getOrDefault(command.options(), FindAndRerankCommand.Options::includeScores, false))
            .withIncludeSortVector(
                getOrDefault(
                    command.options(), FindAndRerankCommand.Options::includeSortVector, false));

    return new TaskGroupAndDeferrables<>(
        taskGroup,
        rerankAccumulator,
        deferredCommandResults.stream()
            .map(RerankingTask.DeferredCommandWithSource::deferredRead)
            .collect(java.util.stream.Collectors.toUnmodifiableList()));
  }

  private TaskGroupAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject> readTasks(
      FindAndRerankPlan plan, List<RerankingTask.DeferredCommandWithSource> deferredReads) {

    // we can run these tasks in parallel
    TaskGroup<IntermediateCollectionReadTask, CollectionSchemaObject> taskGroup =
        new TaskGroup<>(false);

    // Hack: See https://github.com/stargate/data-api/issues/1961
    // copying the hybrid limits on the command context so the find command resolver can pick it up
    // when the command runs later, so we can set the page size to be the same as the limit
    commandContext.setHybridLimits(
        getOrDefault(
            command.options(),
            FindAndRerankCommand.Options::hybridLimits,
            FindAndRerankCommand.HybridLimits.DEFAULT));

    var includeScores =
        getOrDefault(command.options(), FindAndRerankCommand.Options::includeScores, false);
    var includeSortVector =
        getOrDefault(command.options(), FindAndRerankCommand.Options::includeSortVector, false);

    // the deferred vectorize of every leg that needs one, for the embedding task
    List<Deferrable> deferrables = new ArrayList<>();

    // one read per leg, the position of the read task is the index of the leg in the plan
    for (int i = 0; i < plan.legs().size(); i++) {
      var leg = plan.legs().get(i);
      var innerRead =
          leg.buildInnerRead(command.filterDefinition(), includeScores, includeSortVector);

      // this is the action the read should call when done, to pass the command result into the
      // next tasks
      var deferredReadAction =
          DeferredAction.filtered(
                  DeferredCommandResultAction.class,
                  Deferrable.deferred(deferredReads.get(i).deferredRead()))
              .getFirst();

      // The intermediate task will set the sort when we give it the deferred vectorize
      taskGroup.add(
          new IntermediateCollectionReadTask(
              i,
              commandContext.schemaObject(),
              TaskRetryPolicy.NO_RETRY,
              findCommandResolver,
              innerRead.findCommand(),
              innerRead.deferredVectorize(),
              deferredReadAction));
      if (leg.needsVectorize()) {
        deferrables.add(innerRead.deferredVectorize());
      }
    }

    // No accumulator, this will be wrapped in an intermediate composite task
    return new TaskGroupAndDeferrables<>(taskGroup, null, deferrables);
  }
}
