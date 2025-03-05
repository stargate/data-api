package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingAction;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTaskGroupBuilder;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.RowSorter;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableCqlSortClauseResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableMemorySortClauseResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.ValueAction;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <CmdT>
 */
class TableReadDBOperationBuilder<
    CmdT extends ReadCommand & Filterable & Projectable & Sortable & Windowable & VectorSortable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadDBOperationBuilder.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final CommandContext<TableSchemaObject> commandContext;
  private final FilterResolver<CmdT, TableSchemaObject> tableFilterResolver;
  private final TableCqlSortClauseResolver<CmdT> tableCqlSortClauseResolver;
  // we use this in a bunch of places
  private final OperationsConfig operationsConfig;

  // things set in the builder pattern.
  private CqlPagingState cqlPageState = null;
  private CmdT command;
  private Boolean singleResponse = null;

  //  private JsonNamedValueFactory jsonNamedValueFactory = null;
  //  private Boolean ordered = null;
  //  private Boolean returnDocumentResponses = null;

  public TableReadDBOperationBuilder(CommandContext<TableSchemaObject> commandContext) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");

    operationsConfig = commandContext.config().get(OperationsConfig.class);
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
    this.tableCqlSortClauseResolver = new TableCqlSortClauseResolver<>(operationsConfig);
  }

  TableReadDBOperationBuilder<CmdT> withPagingState(CqlPagingState cqlPageState) {
    this.cqlPageState = cqlPageState;
    return this;
  }

  TableReadDBOperationBuilder<CmdT> withCommand(CmdT command) {
    this.command = command;
    return this;
  }

  TableReadDBOperationBuilder<CmdT> withSingleResponse(Boolean singleResponse) {
    this.singleResponse = singleResponse;
    return this;
  }

  public Operation<TableSchemaObject> build() {
    Objects.requireNonNull(command, "command cannot be null");

    // create the read task to see if we need any deferred values
    var readTaskAndDeferrables = createReadTask();

    var allDeferredValues = Deferrable.deferredValues(readTaskAndDeferrables.deferrables);
    if (allDeferredValues.isEmpty()) {
      // basic read, just wrap the tasks in an operation and go
      return new TaskOperation<>(
          readTaskAndDeferrables.readTaskGroup, readTaskAndDeferrables.accumulator);
    }

    // we have some deferred values, e.g. we need to do vectorizing, so we need  to build a
    // hierarchy of task groups for now we only have vectorize actions so quick sanity check
    var allActions = ValueAction.filteredActions(ValueAction.class, allDeferredValues);
    var embeddingActions = ValueAction.filteredActions(EmbeddingAction.class, allDeferredValues);
    if (allActions.size() != embeddingActions.size()) {
      throw new IllegalArgumentException("Unsupported actions in deferred values: " + allActions);
    }

    // Send the EmbeddingAction's to the builder to get back a list of EmbeddingTasks
    // that are linked to the actions they get vectors for.
    var embeddingTaskGroup =
        new EmbeddingTaskGroupBuilder<TableSchemaObject>()
            .withCommandContext(commandContext)
            .withEmbeddingActions(
                allActions.stream().map(action -> (EmbeddingAction) action).toList())
            .withRequestType(EmbeddingProvider.EmbeddingRequestType.SEARCH)
            .build();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "build() - deferred values for vectorizing, returning composite task group with embeddingTaskGroup.size={}, readTaskGroup.size={}",
          embeddingTaskGroup.size(),
          readTaskAndDeferrables.readTaskGroup.size());
    }

    // we want to run a group of embedding tasks and then a group of reads,
    // the two groups are linked by the EmbeddingAction objects
    // Because these are tables we only use the driver retry for the inserts, not task level retry
    return new CompositeTaskOperationBuilder<>(commandContext)
        .withIntermediateTasks(embeddingTaskGroup, TaskRetryPolicy.NO_RETRY)
        .build(
            readTaskAndDeferrables.readTaskGroup,
            TaskRetryPolicy.NO_RETRY,
            readTaskAndDeferrables.accumulator);
  }

  private ReadTaskAndDeferrables createReadTask() {

    Objects.requireNonNull(cqlPageState, "cqlPageState cannot be null");
    Objects.requireNonNull(singleResponse, "singleResponse cannot be null");

    var taskBuilder =
        new TableReadDBTaskBuilder(commandContext.schemaObject())
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new)
            .withPagingState(cqlPageState);

    var orderByWithWarnings = tableCqlSortClauseResolver.resolve(commandContext, command);
    taskBuilder.withOrderBy(orderByWithWarnings);

    // if the user did not provide a limit, we read all the possible rows. Paging is then handled
    // by the driver pagination
    int commandLimit =
        command.limit().orElseGet(() -> orderByWithWarnings.target().getDefaultLimit());

    // and then if we need to do in memory sorting
    var inMemorySort =
        new TableMemorySortClauseResolver<>(
                operationsConfig,
                orderByWithWarnings.target(),
                command.skip().orElse(0),
                // Math.min is used because the max documents the api return is
                // `operationsConfig.defaultPageSize()`
                Math.min(commandLimit, operationsConfig.defaultPageSize()),
                cqlPageState)
            .resolve(commandContext, command);
    taskBuilder.withSorter(inMemorySort);

    // if in memory sort the limit to use in select query will be
    // `operationsConfig.maxDocumentSortCount() + 1` so we read all the docs into memory and then
    // sort
    taskBuilder.withCqlBuilderOption(
        CQLOption.ForSelect.limit(
            inMemorySort.target() == RowSorter.NO_OP
                ? commandLimit
                : operationsConfig.maxDocumentSortCount() + 1));

    // the columns the user wants
    // NOTE: the TableProjection is doing double duty as the select and the operation projection
    var projection =
        TableProjection.fromDefinition(OBJECT_MAPPER, command, commandContext.schemaObject());

    taskBuilder.withSelect(WithWarnings.of(projection));
    taskBuilder.withProjection(projection);

    // We will want the ability to turn a single find command into multiple tasks, this is why
    // the builder builds on the where clause
    var where =
        TableWhereCQLClause.forSelect(
            commandContext.schemaObject(),
            tableFilterResolver.resolve(commandContext, command));

    // always parallel processing for the taskgroup
    var taskGroup = new TaskGroup<>(taskBuilder.build(where.target()));

    return new ReadTaskAndDeferrables(
        taskGroup,
        ReadDBTaskPage.accumulator(commandContext)
            .singleResponse(singleResponse)
            .mayReturnVector(command),
        List.of(orderByWithWarnings.target()));
  }

  private record ReadTaskAndDeferrables(
      TaskGroup<ReadDBTask<TableSchemaObject>, TableSchemaObject> readTaskGroup,
      TaskAccumulator<ReadDBTask<TableSchemaObject>, TableSchemaObject> accumulator,
      List<Deferrable> deferrables) {}
}
