package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttempt;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableProjection;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableReadAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableWhereCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableCqlSortClauseResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableMemorySortClauseResolver;

/**
 * NOTE: This was intended to be a base class for the Find and FindOne resolvers, but I could not
 * work out how to get subclassing to work with the dependency injection framework. So, I have left
 * it as a standalone class for now. - aaron 4 nov 2024
 *
 * @param <CmdT>
 */
class ReadCommandResolver<
    CmdT extends ReadCommand & Filterable & Projectable & Sortable & Windowable> {

  private final ObjectMapper objectMapper;
  private final OperationsConfig operationsConfig;
  private final FilterResolver<CmdT, TableSchemaObject> tableFilterResolver;
  private final TableCqlSortClauseResolver<CmdT> tableCqlSortClauseResolver;

  protected ReadCommandResolver(ObjectMapper objectMapper, OperationsConfig operationsConfig) {
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;

    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
    this.tableCqlSortClauseResolver = new TableCqlSortClauseResolver<>(operationsConfig);
  }

  /**
   * Build a read operation for the command and context.
   *
   * <p>Params are the specific per command values
   *
   * @param commandContext
   * @param command
   * @param commandSkip Number of rows the command wants to skip
   * @param commandLimit Number of rows the command wants to return
   * @param cqlPageState The CQL paging state, if any, must be non null
   * @param pageBuilder The page builder to use, the caller should configure this with any command
   *     specific options before passing, such as single document mode
   * @return Configured read operation
   */
  protected GenericOperation<TableSchemaObject, ReadAttempt<TableSchemaObject>> buildReadOperation(
      CommandContext<TableSchemaObject> commandContext,
      CmdT command,
      int commandSkip,
      int commandLimit,
      CqlPagingState cqlPageState,
      ReadAttemptPage.Builder<TableSchemaObject> pageBuilder) {

    var attemptBuilder = new TableReadAttemptBuilder(commandContext.schemaObject());

    attemptBuilder.addBuilderOption(CQLOption.ForSelect.limit(commandLimit));
    if (cqlPageState != null) {
      attemptBuilder.addPagingState(cqlPageState);
    }
    // work out the CQL order by
    var orderByWithWarnings = tableCqlSortClauseResolver.resolve(commandContext, command);
    attemptBuilder.addOrderBy(orderByWithWarnings);

    // and then if we need to do in memory sorting
    attemptBuilder.addSorter(
        new TableMemorySortClauseResolver<>(
                operationsConfig, orderByWithWarnings.target(), commandSkip, commandLimit)
            .resolve(commandContext, command));

    // the columns the user wants
    // NOTE: the TableProjection is doing double duty as the select and the operation projection
    var projection =
        TableProjection.fromDefinition(
            objectMapper, command.tableProjectionDefinition(), commandContext.schemaObject());
    attemptBuilder.addSelect(WithWarnings.of(projection));
    attemptBuilder.addProjection(projection);

    // TODO, we may want the ability to resolve API filter clause into multiple
    // dbLogicalExpressions, which will map into multiple readAttempts
    var where =
        TableWhereCQLClause.forSelect(
            commandContext.schemaObject(),
            tableFilterResolver.resolve(commandContext, command).target());

    var attempts = new OperationAttemptContainer<>(attemptBuilder.build(where));

    // the common page builder options
    pageBuilder
        .debugMode(commandContext.getConfig(DebugModeConfig.class).enabled())
        .useErrorObjectV2(commandContext.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }
}
