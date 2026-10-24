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
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the operation for a {@link FindAndRerankCommand} from the {@link FindAndRerankPlan} made
 * by {@link FindAndRerankPlanner}.
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
    // Making the deferrables here so we can associate them with read types that will fill them
    var deferredVectorRead =
        new RerankingTask.DeferredCommandWithSource(
            Rank.RankSource.VECTOR, new DeferredCommandResult());
    var deferredBM25Read =
        new RerankingTask.DeferredCommandWithSource(
            Rank.RankSource.BM25, new DeferredCommandResult());

    var rerankTasksAndDeferrables = rerankTasks(List.of(deferredBM25Read, deferredVectorRead));

    // Step 2 - we need to read the data from the collections, we are wrapping the old collections
    // in the new tasks so we do not change the collection code
    var readTasksAndDeferrables = readTasks(deferredVectorRead, deferredBM25Read);

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

    var rerankStep = plan.rerank();
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
      RerankingTask.DeferredCommandWithSource deferredVectorRead,
      RerankingTask.DeferredCommandWithSource deferredBM25Read) {

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

    // these are the actions the reads should call when done, to pass the command result into the
    // next tasks
    var deferredBM25ReadAction =
        DeferredAction.filtered(
                DeferredCommandResultAction.class,
                Deferrable.deferred(deferredBM25Read.deferredRead()))
            .getFirst();
    var deferredVectorReadAction =
        DeferredAction.filtered(
                DeferredCommandResultAction.class,
                Deferrable.deferred(deferredVectorRead.deferredRead()))
            .getFirst();

    // The BM25 read
    var bm25Read = buildBm25Read(deferredBM25ReadAction);
    if (bm25Read != null) {
      taskGroup.add(bm25Read);
    }

    // always a vector or vectorize read
    var vectorReadAndDeferrables = buildVectorRead(deferredVectorReadAction);
    taskGroup.add(vectorReadAndDeferrables.task());

    // No accumulator, this will be wrapped in an intermediate composite task
    return new TaskGroupAndDeferrables<>(taskGroup, null, vectorReadAndDeferrables.deferrables());
  }

  private IntermediateCollectionReadTask buildBm25Read(DeferredCommandResultAction deferredAction) {

    var lexicalLeg = leg(RerankLegType.LEXICAL_LEG);
    if (lexicalLeg.isEmpty()) {
      // we can fake it now, the value will be waiting when the rerank command comes to get it
      deferredAction.setEmptyMultiDocumentResponse();
      return null;
    }

    var bm25SortClause =
        new SortClause(
            List.of(SortExpression.collectionLexicalSort(lexicalLeg.get().lexicalText())));
    var bm25ReadCommand =
        new FindCommand(
            command.filterDefinition(),
            INCLUDE_ALL_PROJECTION,
            SortDefinition.wrap(bm25SortClause),
            buildFindOptions(lexicalLeg.get()));

    return new IntermediateCollectionReadTask(
        0,
        commandContext.schemaObject(),
        TaskRetryPolicy.NO_RETRY,
        findCommandResolver,
        bm25ReadCommand,
        null,
        deferredAction);
  }

  /** Builder either a vectorize or BYO vector read. */
  private TaskAndDeferrables<IntermediateCollectionReadTask, CollectionSchemaObject>
      buildVectorRead(DeferredCommandResultAction deferredAction) {

    // sanity check, the planner always makes a VECTOR_LEG
    // TODO: GH #2597 - the builder should not need a VECTOR_LEG
    var vectorLeg =
        leg(RerankLegType.VECTOR_LEG)
            .orElseThrow(
                () -> new IllegalArgumentException("buildVectorRead() - no vector or vectorize"));

    // we can sort with either vectorize OR a BYO vector
    var sortClause = new SortClause(new ArrayList<>());
    DeferredVectorize deferredVectorize = null;

    var vectorize = plan.vectorize();
    if (vectorize != null) {
      // pass the vector sort clause through so it will be updated when we get the vector
      deferredVectorize =
          new DeferredVectorize(
              vectorize.text(), vectorize.dimension(), vectorize.vectorizeDefinition(), sortClause);
    } else {
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
            1,
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

  /** The leg of the plan with this type, there is at most one of each type. */
  private Optional<FindAndRerankPlan.RerankLeg> leg(RerankLegType legType) {
    return plan.legs().stream().filter(leg -> leg.legType() == legType).findFirst();
  }
}
