package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.reranking.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroupAndDeferrables;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

/**

 */
class FindAndRerankOperationBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(FindAndRerankOperationBuilder.class);


  private final CommandContext<CollectionSchemaObject> commandContext;

  // we use this in a bunch of places
  private final OperationsConfig operationsConfig;

  // things set in the builder pattern.
  private FindAndRerankCommand   command;
  private FindCommandResolver commandResolver;

  public FindAndRerankOperationBuilder(CommandContext<CollectionSchemaObject> commandContext) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");

    operationsConfig = commandContext.config().get(OperationsConfig.class);
  }

  public FindAndRerankOperationBuilder withCommand(FindAndRerankCommand command) {
    this.command = command;
    return this;
  }

  public FindAndRerankOperationBuilder withCommandResolver(FindCommandResolver commandResolver) {
    this.commandResolver = commandResolver;
    return this;
  }

  public Operation<TableSchemaObject> build() {

    Objects.requireNonNull(command, "command cannot be null");

    // Step 1 - we need a reranking task and the deferrable actions to do the intermediate reads
    var rerankTasksAndDeferrables = rerankTasks();

    // Step 2 - we need to read the data from the collections, we are wrapping the old collections in the
    // new tasks so we do not change the collectionc ode
    var readTasksAndDeferrables

  }

  private TaskGroupAndDeferrables<RerankingTask<CollectionSchemaObject>, CollectionSchemaObject> rerankTasks() {

    // TODO : Get the reranking provider
    Object rerankingProvider = new Object();

    // TODO: look at the command to see what we want to do both the ANN and BM25 reads
    // we need a deferred action for each read
    // at the moment we dont
    List<DeferredCommandResult> deferredCommandResults = List.of(new DeferredCommandResult(), new DeferredCommandResult());
    List<Deferrable> deferrables = new ArrayList<>(deferredCommandResults);

    // todo: move to a builder pattern, mosty to make it eaier to manage the task position and retry policy
    RerankingTask<CollectionSchemaObject> task = new RerankingTask<>(
        0,
        commandContext.schemaObject(),
        TaskRetryPolicy.NO_RETRY,
        rerankingProvider,
        deferredCommandResults
    );

    // there is only 1 task, but making it clear that we want sequential for this step
    TaskGroup<RerankingTask<CollectionSchemaObject>, CollectionSchemaObject> taskGroup = new TaskGroup<>(true);
    taskGroup.add(task);

    return new TaskGroupAndDeferrables<>(taskGroup, RerankingTaskPage.accumulator(commandContext), deferrables);
  }

  private TaskGroupAndDeferrables<IntermediateCollectionReadTask<CollectionSchemaObject>, CollectionSchemaObject> readTasks(List<DeferredCommandResultAction>  deferredCommandResults) {

    var deferredBM25Action = deferredCommandResults.get(0);
    var deferredVectorAction = deferredCommandResults.get(1);

    // We build a fake FindCommand, the final step for the IntermediateCollectionReadTask will be to
    // get the vector for the sort if needed
    var findCommandOptions = new FindCommand.Options(
        getOrDefault(command.options(), FindAndRerankCommand.Options::limit, 10),
        0,
        null,
        false,
        true);

    // The BM25 read
    // TODO: get the BM 25 sort from the findAndReRank command
    var bm25SortTerm = "cows milk cheese";
    var bm25SortClause = new SortClause(List.of(new SortExpression("$lexical", true, null, bm25SortTerm)));
    var bm25ReadCommand = new FindCommand(command.filterSpec(), command.projectionDefinition(), bm25SortClause, findCommandOptions);
    var bm25IntermediateReadTask = new IntermediateCollectionReadTask(
        0,
        commandContext.schemaObject(),
        TaskRetryPolicy.NO_RETRY,
        commandResolver,
        bm25ReadCommand,
        null,
        deferredBM25Action
    );

    //The Vector read
    // TODO: get the vectorize sort term from the findAndReRank command
    var vectorizeText = "I like cheese";
    VectorColumnDefinition vectorDef =
        commandContext.schemaObject().vectorConfig().getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();
    var deferredVectorize = new DeferredVectorize(vectorizeText, vectorDef.vectorSize(), vectorDef.vectorizeDefinition());

    var vectorIntermediateReadTask = new IntermediateCollectionReadTask(
        0,
        commandContext.schemaObject(),
        TaskRetryPolicy.NO_RETRY,
        commandResolver,
        bm25ReadCommand,
        deferredVectorize,
        deferredBM25Action
    );

    // we can run these tasks in parallel
    TaskGroup<IntermediateCollectionReadTask, CollectionSchemaObject> taskGroup = new TaskGroup<>(false);
    taskGroup.add(bm25IntermediateReadTask);
    taskGroup.add(vectorIntermediateReadTask);

    // No accumulator, this will be wrapped in an intermediate composite task
    return new TaskGroupAndDeferrables<>(taskGroup, null, List.of((Deferrable) deferredVectorize));
  }
}
