package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.RowSorter;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableCqlSortClauseResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableMemorySortClauseResolver;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * NOTE: This was intended to be a base class for the Find and FindOne resolvers, but I could not
 * work out how to get subclassing to work with the dependency injection framework. So, I have left
 * it as a standalone class for now. - aaron 4 nov 2024
 *
 * @param <CmdT>
 */
class TableReadDBOperationBuilder<
    CmdT extends ReadCommand & Filterable & Projectable & Sortable & Windowable & VectorSortable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadDBOperationBuilder.class);

//  private final ObjectMapper objectMapper;



  private final CommandContext<TableSchemaObject> commandContext;
  private final FilterResolver<CmdT, TableSchemaObject> tableFilterResolver;
  private final TableCqlSortClauseResolver<CmdT> tableCqlSortClauseResolver;
  // we use this in a bunch of places
  private final OperationsConfig operationsConfig;

  private CqlPagingState cqlPageState = null;

//  private JsonNamedValueFactory jsonNamedValueFactory = null;
//  private Boolean ordered = null;
//  private Boolean returnDocumentResponses = null;

  public TableReadDBOperationBuilder(CommandContext<TableSchemaObject> commandContext) {
    this.commandContext = Objects.requireNonNull(commandContext, "commandContext cannot be null");

    operationsConfig =  commandContext.config().get(OperationsConfig.class);
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
    this.tableCqlSortClauseResolver = new TableCqlSortClauseResolver<>(operationsConfig);
  }

//  protected TableReadDBOperationBuilder(ObjectMapper objectMapper, OperationsConfig operationsConfig) {
//    this.objectMapper = objectMapper;
//    this.operationsConfig = operationsConfig;
//
//    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
//    this.tableCqlSortClauseResolver = new TableCqlSortClauseResolver<>(operationsConfig);
//  }

  TableReadDBOperationBuilder<CmdT, SchemaT> withPagingState(CqlPagingState cqlPageState) {
    this.cqlPageState = cqlPageState;
    return this;
  }


  protected TaskOperation<ReadDBTask<TableSchemaObject>, TableSchemaObject> buildReadOperation(
      CmdT command,
      ReadDBTaskPage.Accumulator<TableSchemaObject> taskAccumulator) {

    Objects.requireNonNull(cqlPageState, "cqlPageState cannot be null");

    var taskBuilder = new TableReadDBTaskBuilder(commandContext.schemaObject());
    taskBuilder.withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    if (cqlPageState != null) {
      taskBuilder.withPagingState(cqlPageState);
    }

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
    taskBuilder.withBuilderOption(
        CQLOption.ForSelect.limit(
            inMemorySort.target() == RowSorter.NO_OP
                ? commandLimit
                : operationsConfig.maxDocumentSortCount() + 1));

    // the columns the user wants
    // NOTE: the TableProjection is doing double duty as the select and the operation projection
    var projection =
        TableProjection.fromDefinition(objectMapper, command, commandContext.schemaObject());

    taskBuilder.withSelect(WithWarnings.of(projection));
    taskBuilder.withProjection(projection);

    // We will want the ability to turn a single find command into multiple tasks, this is why
    // the builder builds on the where clause
    var where =
        TableWhereCQLClause.forSelect(
            commandContext.schemaObject(),
            tableFilterResolver.resolve(commandContext, command).target());

    // parallel processing all the of time
    var taskGroup = new TaskGroup<>(taskBuilder.build(where));

    return new TaskOperation<>(taskGroup, taskAccumulator);
  }
}
