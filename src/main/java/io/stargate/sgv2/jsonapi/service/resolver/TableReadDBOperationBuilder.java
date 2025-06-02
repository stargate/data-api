package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingOperationFactory;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.RowSorter;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.*;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableCqlSortClauseResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableMemorySortClauseResolver;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates resolving a read command into a {@link Operation}, which includes building the tasks
 * and any deferrables.
 *
 * <p>We use this for the Read commands because they are complex and have a lot more to put together
 * than other commands like an insert.
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
            commandContext.schemaObject(), tableFilterResolver.resolve(commandContext, command));

    // always parallel processing for the taskgroup
    var taskGroup = new TaskGroup<>(taskBuilder.build(where.target()));

    var tasksAndDeferrables =
        new TaskGroupAndDeferrables<>(
            taskGroup,
            ReadDBTaskPage.accumulator(commandContext)
                .singleResponse(singleResponse)
                .mayReturnVector(command),
            List.of(orderByWithWarnings.target()));

    return EmbeddingOperationFactory.createOperation(commandContext, tasksAndDeferrables);
  }
}
