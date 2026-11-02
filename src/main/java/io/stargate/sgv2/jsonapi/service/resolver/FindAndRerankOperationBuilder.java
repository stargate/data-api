package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
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
 * hack in readTasks() (GH #1961). The operation runs one read for each leg of the plan, then
 * reranks the documents from all the reads.
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
    // Making one deferrable for each leg of the plan, in the plan order, so the read built for the
    // leg at the same index fills it
    var deferredReads =
        plan.legs().stream()
            .map(
                leg ->
                    new RerankingTask.DeferredCommandWithSource(
                        rankSource(leg), new DeferredCommandResult()))
            .toList();

    var rerankTasksAndDeferrables = rerankTasks(deferredReads);

    // Step 2 - we need to read the data from the collections, we are wrapping the old collections
    // in the new tasks so we do not change the collection code
    var readTasksAndDeferrables = readTasks(deferredReads);

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
      rerankTasks(List<RerankingTask.DeferredCommandWithSource> deferredCommandResults) {

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
            deferredCommandResults,
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
        deferredCommandResults.stream()
            .map(RerankingTask.DeferredCommandWithSource::deferredRead)
            .collect(java.util.stream.Collectors.toUnmodifiableList()));
  }

  private TaskGroupAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject> readTasks(
      List<RerankingTask.DeferredCommandWithSource> deferredReads) {

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

    // the deferred vectorize of the vector read, when the plan vectorizes
    var deferrables = new ArrayList<Deferrable>();

    // one read for each leg, the read fills the deferred read at the index of its leg
    // NOTE: the reads are in the plan order, so the vector read comes before the lexical read, the
    // old builder put the BM25 read first. The order does not change the result: the reads run in
    // parallel, and the reranking task sorts the documents by their scores and then by _id.
    for (int position = 0; position < plan.legs().size(); position++) {
      var leg = plan.legs().get(position);

      // this is the action the read should call when done, to pass the command result into the
      // next tasks
      var deferredReadAction =
          DeferredAction.filtered(
                  DeferredCommandResultAction.class,
                  Deferrable.deferred(deferredReads.get(position).deferredRead()))
              .getFirst();

      var readAndDeferrables =
          switch (leg.legType()) {
            case VECTOR_LEG -> buildVectorRead(position, leg, deferredReadAction);
            case LEXICAL_LEG -> buildBm25Read(position, leg, deferredReadAction);
              // sanity check, rankSource() has already thrown for a FILTER_LEG
            case FILTER_LEG ->
                throw new IllegalStateException(
                    "readTasks() - the builder cannot read with a FILTER_LEG yet, leg=%s"
                        .formatted(leg));
          };
      taskGroup.add(readAndDeferrables.task());
      deferrables.addAll(readAndDeferrables.deferrables());
    }

    // No accumulator, this will be wrapped in an intermediate composite task
    return new TaskGroupAndDeferrables<>(taskGroup, null, deferrables);
  }

  private TaskAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject> buildBm25Read(
      int position,
      FindAndRerankPlan.RerankLeg lexicalLeg,
      DeferredCommandResultAction deferredAction) {

    var bm25SortClause =
        new SortClause(List.of(SortExpression.collectionLexicalSort(lexicalLeg.lexicalText())));
    var bm25ReadCommand =
        new FindCommand(
            command.filterDefinition(),
            INCLUDE_ALL_PROJECTION,
            SortDefinition.wrap(bm25SortClause),
            buildFindOptions(lexicalLeg));

    return new TaskAndDeferrables<>(
        new IntermediateCollectionReadTask(
            position,
            commandContext.schemaObject(),
            TaskRetryPolicy.NO_RETRY,
            findCommandResolver,
            bm25ReadCommand,
            null,
            deferredAction));
  }

  /** Builder either a vectorize or BYO vector read. */
  private TaskAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject>
      buildVectorRead(
          int position,
          FindAndRerankPlan.RerankLeg vectorLeg,
          DeferredCommandResultAction deferredAction) {

    // we can sort with either vectorize OR a BYO vector
    var sortClause = new SortClause(new ArrayList<>());
    DeferredVectorize deferredVectorize = null;

    var vectorizeStep = plan.vectorizeStep();
    if (vectorizeStep != null) {
      // pass the vector sort clause through so it will be updated when we get the vector
      deferredVectorize =
          new DeferredVectorize(
              vectorizeStep.text(),
              vectorizeStep.dimension(),
              vectorizeStep.vectorizeDefinition(),
              sortClause);
    } else {
      // the plan is valid, so the VECTOR_LEG has a vector when there is no vectorize step
      sortClause.sortExpressions().add(SortExpression.collectionVectorSort(vectorLeg.vector()));
    }

    // The intermediate task will set the sort when we give it the deferred vectorize
    var vectorReadCommand =
        new FindCommand(
            command.filterDefinition(),
            INCLUDE_ALL_PROJECTION,
            SortDefinition.wrap(sortClause),
            buildFindOptions(vectorLeg));
    var readTask =
        new IntermediateCollectionReadTask(
            position,
            commandContext.schemaObject(),
            TaskRetryPolicy.NO_RETRY,
            findCommandResolver,
            vectorReadCommand,
            deferredVectorize,
            deferredAction);

    return deferredVectorize == null
        ? new TaskAndDeferrables<>(readTask)
        : new TaskAndDeferrables<>(readTask, deferredVectorize);
  }

  private FindCommand.Options buildFindOptions(FindAndRerankPlan.RerankLeg leg) {
    return new FindCommand.Options(
        leg.readLimit(), 0, null, plan.includeScores(), plan.includeSortVector());
  }

  /** The source of the ranks for the documents the read for the leg finds. */
  private Rank.RankSource rankSource(FindAndRerankPlan.RerankLeg leg) {
    return switch (leg.legType()) {
      case VECTOR_LEG -> Rank.RankSource.VECTOR;
      case LEXICAL_LEG -> Rank.RankSource.BM25;
        // sanity check, the planner does not make a FILTER_LEG yet
        // TODO: GH #2603 - add a rank source for the FILTER_LEG, and build its read in readTasks()
      case FILTER_LEG ->
          throw new IllegalStateException(
              "rankSource() - the builder cannot read with a FILTER_LEG yet, leg=%s"
                  .formatted(leg));
    };
  }
}
