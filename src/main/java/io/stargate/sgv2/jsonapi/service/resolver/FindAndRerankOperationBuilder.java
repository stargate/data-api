package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskGroupBuilder;
import io.stargate.sgv2.jsonapi.service.operation.reranking.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
class FindAndRerankOperationBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FindAndRerankOperationBuilder.class);

  // Need a Projection for the inner finds, it's too complicated to merge the user projection
  // and what we need, because the user may be running a projection to hide fields, which
  // cannot then include fields, so we just use the * wildcard to include all fields for now
  private static final JsonNode INCLUDE_ALL_PROJECTION =
      JsonNodeFactory.instance.objectNode().put("*", 1);

  private final CommandContext<CollectionSchemaObject> commandContext;

  // we use this in a bunch of places
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

    checkSupported();

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

  /**
   * Check the collection supports hybrid search with the features the request uses, throw if it
   * does not
   */
  private void checkSupported() {

    if (isVectorSort() || isVectorizeSort()) {
      if (!commandContext.schemaObject().vectorConfig().vectorEnabled()) {
        throw SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION.get(
            errVars(commandContext.schemaObject()));
      }
    }

    if (isVectorizeSort()) {
      // the service definition is on the $vectorize field, not $vector
      var vectorizeEnabled =
          commandContext
              .schemaObject()
              .vectorConfig()
              .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
              .map(VectorColumnDefinition::vectorizeDefinition)
              .isPresent();

      if (!vectorizeEnabled) {
        throw SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION.get(
            errVars(commandContext.schemaObject()));
      }
    }

    if (isLexicalSort()) {
      if (!commandContext.schemaObject().lexicalConfig().enabled()) {
        throw SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.get(
            errVars(commandContext.schemaObject()));
      }
    }

    if (!commandContext.schemaObject().rerankingConfig().enabled()) {
      // TODO: more info in the error
      throw RequestException.Code.UNSUPPORTED_RERANKING_COMMAND.get();
    }
    // Read is not supported for rerank model at END_OF_LIFE support status.
    var rerankingProvidersConfig = commandContext.rerankingProviderFactory().getRerankingConfig();
    var modelConfig =
        rerankingProvidersConfig.filterByRerankServiceDef(
            commandContext.schemaObject().rerankingConfig().rerankServiceDef());
    // Validate if the model is END_OF_LIFE
    if (modelConfig.apiModelSupport().status() == ApiModelSupport.SupportStatus.END_OF_LIFE) {
      throw SchemaException.Code.END_OF_LIFE_AI_MODEL.get(
          Map.of(
              "model",
              modelConfig.name(),
              "modelStatus",
              modelConfig.apiModelSupport().status().name(),
              "message",
              modelConfig
                  .apiModelSupport()
                  .message()
                  .orElse("The model is no longer supported (reached its end-of-life).")));
    }
  }

  private TaskGroupAndDeferrables<RerankingTask<CollectionSchemaObject>, CollectionSchemaObject>
      rerankTasks(List<RerankingTask.DeferredCommandWithSource> deferredCommandResults) {

    // Previous code will check reranking is supported
    var providerConfig = commandContext.schemaObject().rerankingConfig().rerankServiceDef();
    RerankingProvider rerankingProvider =
        commandContext
            .rerankingProviderFactory()
            .create(
                commandContext.requestContext().tenant(),
                commandContext.requestContext().authToken(),
                providerConfig.provider(),
                providerConfig.modelName(),
                providerConfig.authentication(),
                commandContext.commandName());

    // todo: move to a builder pattern, mosty to make it easier to manage the task position and
    // retry
    // policy
    int commandLimit = getOrDefault(command.options(), FindAndRerankCommand.Options::limit, 10);
    RerankingTask<CollectionSchemaObject> task =
        new RerankingTask<>(
            0,
            commandContext.schemaObject(),
            TaskRetryPolicy.NO_RETRY,
            rerankingProvider,
            RerankingQuery.create(command),
            passageLocator(),
            command.buildProjector(),
            deferredCommandResults,
            commandLimit);

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
            .collect(Collectors.toUnmodifiableList()));
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

    if (!isLexicalSort()) {
      // we can fake it now, the value will be waiting when the rerank command comes to get it
      deferredAction.setEmptyMultiDocumentResponse();
      return null;
    }

    var bm25SortTerm = command.sortClause().lexicalSort();
    var bm25SortClause =
        new SortClause(List.of(SortExpression.collectionLexicalSort(bm25SortTerm)));
    var bm25ReadCommand =
        new FindCommand(
            command.filterDefinition(),
            INCLUDE_ALL_PROJECTION,
            SortDefinition.wrap(bm25SortClause),
            buildFindOptions(false));

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

    // we can sort with either vectorize OR a BYO vector
    var sortClause = new SortClause(new ArrayList<>());
    DeferredVectorize deferredVectorize = null;

    if (isVectorizeSort()) {

      VectorColumnDefinition vectorDef =
          commandContext
              .schemaObject()
              .vectorConfig()
              .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
              .orElseThrow();

      // pass the vector sort clause through so it will be updated when we get the vector
      deferredVectorize =
          new DeferredVectorize(
              command.sortClause().vectorizeSort(),
              vectorDef.vectorSize(),
              vectorDef.vectorizeDefinition(),
              sortClause);
    } else if (isVectorSort()) {
      sortClause
          .sortExpressions()
          .add(SortExpression.collectionVectorSort(command.sortClause().vectorSort()));
    } else {
      throw new IllegalArgumentException("buildVectorRead() - no vector or vectorize");
    }

    // The intermediate task will set the sort when we give it the deferred vectorize
    var vectorReadCommand =
        new FindCommand(
            command.filterDefinition(),
            INCLUDE_ALL_PROJECTION,
            SortDefinition.wrap(sortClause),
            buildFindOptions(true));
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

  private FindCommand.Options buildFindOptions(boolean forVectorRead) {

    var hybridLimits =
        getOrDefault(
            command.options(),
            FindAndRerankCommand.Options::hybridLimits,
            FindAndRerankCommand.HybridLimits.DEFAULT);

    var findLimit = forVectorRead ? hybridLimits.vectorLimit() : hybridLimits.lexicalLimit();

    return new FindCommand.Options(
        findLimit,
        0,
        null,
        getOrDefault(command.options(), FindAndRerankCommand.Options::includeScores, false),
        getOrDefault(command.options(), FindAndRerankCommand.Options::includeSortVector, false));
  }

  private PathMatchLocator passageLocator() {

    var rerankOn = getOrDefault(command.options(), FindAndRerankCommand.Options::rerankOn, null);
    var isRerankOn = rerankOn != null && !rerankOn.isBlank();

    String finalRerankField = null;

    if (isVectorizeSort()) {
      // use the vectorize field, unless the user has overridden
      finalRerankField = isRerankOn ? rerankOn : VECTOR_EMBEDDING_TEXT_FIELD;
    } else if (isRerankOn) {
      // user has to provide a field to rererank on
      finalRerankField = rerankOn;

    } else {
      throw new IllegalArgumentException("rerankOn() - rerankOn required and not specified");
    }

    return PathMatchLocator.forPath(finalRerankField);
  }

  private boolean isLexicalSort() {
    return command.sortClause().lexicalSort() != null;
  }

  private boolean isVectorizeSort() {
    return command.sortClause().vectorizeSort() != null;
  }

  private boolean isVectorSort() {
    return command.sortClause().vectorSort() != null;
  }
}
