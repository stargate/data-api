package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterLogicalExpression;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneAndDeleteCommand } */
@ApplicationScoped
public class FindOneAndDeleteCommandResolver implements CommandResolver<FindOneAndDeleteCommand> {
  private final DocumentShredder documentShredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final CollectionFilterResolver<FindOneAndDeleteCommand> collectionFilterResolver;

  @Inject
  public FindOneAndDeleteCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      DocumentShredder documentShredder,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.objectMapper = objectMapper;
    this.documentShredder = documentShredder;
    this.operationsConfig = operationsConfig;

    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<FindOneAndDeleteCommand> getCommandClass() {
    return FindOneAndDeleteCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneAndDeleteCommand command) {
    FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);
    final DocumentProjector documentProjector = command.buildProjector();
    // return
    return DeleteCollectionOperation.deleteOneAndReturn(
        ctx, findCollectionOperation, operationsConfig.lwt().retries(), documentProjector);
  }

  private FindCollectionOperation getFindOperation(
      CommandContext<CollectionSchemaObject> commandContext, FindOneAndDeleteCommand command) {
    DBFilterLogicalExpression dbFilterLogicalExpression =
        collectionFilterResolver.resolve(commandContext, command);

    final SortClause sortClause = command.sortClause();
    SchemaValidatable.maybeValidate(commandContext, sortClause);

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);
    var indexUsage = commandContext.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;

    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        dbFilterLogicalExpression,
        indexUsage);
    if (vector != null) {
      return FindCollectionOperation.vsearchSingle(
          commandContext,
          dbFilterLogicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          false);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sortedSingle(
          commandContext,
          dbFilterLogicalExpression,
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
    } else {
      return FindCollectionOperation.unsortedSingle(
          commandContext,
          dbFilterLogicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          false);
    }
  }
}
