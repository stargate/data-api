package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Resolves the {@link DeleteOneCommand } DeleteOne command implements Filterable to identify the
 * record to be deleted, Based on the filter condition a record will deleted
 */
@ApplicationScoped
public class DeleteOneCommandResolver implements CommandResolver<DeleteOneCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final FilterResolver<DeleteOneCommand, CollectionSchemaObject> collectionFilterResolver;
  private final FilterResolver<DeleteOneCommand, TableSchemaObject> tableFilterResolver;

  @Inject
  public DeleteOneCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {

    this.operationsConfig = operationsConfig;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, DeleteOneCommand command) {

    // Sort clause is not supported for table deleteOne command.
    if (!command.sortClause(commandContext).isEmpty()) {
      throw SortException.Code.UNSUPPORTED_SORT_FOR_TABLE_DELETE_COMMAND.get(
          errVars(commandContext.schemaObject(), map -> {}));
    }

    TableDeleteDBTaskBuilder taskBuilder =
        new TableDeleteDBTaskBuilder(commandContext.schemaObject())
            .withDeleteOne(true)
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    // need to update so we use WithWarnings correctly
    var where =
        TableWhereCQLClause.forDelete(
            commandContext.schemaObject(),
            tableFilterResolver.resolve(commandContext, command).target());

    var tasks = new TaskGroup<>(taskBuilder.build(where));
    return new TaskOperation<>(tasks, DeleteDBTaskPage.accumulator(commandContext));
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, DeleteOneCommand command) {

    FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);
    return DeleteCollectionOperation.delete(
        ctx, findCollectionOperation, 1, operationsConfig.lwt().retries());
  }

  @Override
  public Class<DeleteOneCommand> getCommandClass() {
    return DeleteOneCommand.class;
  }

  private FindCollectionOperation getFindOperation(
      CommandContext<CollectionSchemaObject> commandContext, DeleteOneCommand command) {

    var dbLogicalExpression = collectionFilterResolver.resolve(commandContext, command).target();

    final SortClause sortClause = command.sortClause(commandContext);
    if (sortClause != null) {
      sortClause.validate(commandContext.schemaObject());
    }

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);
    var indexUsage = commandContext.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;

    addToMetrics(
        meterRegistry,
        commandContext.requestContext(),
        jsonApiMetricsConfig,
        command,
        dbLogicalExpression,
        indexUsage);
    if (vector != null) {
      return FindCollectionOperation.vsearchSingle(
          commandContext,
          dbLogicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.KEY,
          objectMapper,
          vector,
          false);
    }

    // BM25 search / sort?
    SortExpression bm25Expr = SortClauseUtil.resolveBM25Search(sortClause);
    if (bm25Expr != null) {
      return FindCollectionOperation.bm25Single(
          commandContext,
          dbLogicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.KEY,
          objectMapper,
          bm25Expr);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sortedSingle(
          commandContext,
          dbLogicalExpression,
          DocumentProjector.includeAllProjector(),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          CollectionReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          0,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          operationsConfig.maxDocumentSortCount(),
          false);
    }
    // Otherwise non-sorted
    return FindCollectionOperation.unsortedSingle(
        commandContext,
        dbLogicalExpression,
        DocumentProjector.includeAllProjector(),
        CollectionReadType.KEY,
        objectMapper,
        false);
  }
}
