package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link UpdateOneCommand } */
@ApplicationScoped
public class UpdateOneCommandResolver extends FilterableResolver<UpdateOneCommand>
    implements CommandResolver<UpdateOneCommand> {
  private final Shredder shredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public UpdateOneCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      Shredder shredder,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.operationsConfig = operationsConfig;

    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public Class<UpdateOneCommand> getCommandClass() {
    return UpdateOneCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, UpdateOneCommand command) {
    FindOperation findOperation = getFindOperation(ctx, command);

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());

    // resolve upsert
    UpdateOneCommand.Options options = command.options();
    boolean upsert = options != null && options.upsert();

    // return op
    return new ReadAndUpdateOperation(
        ctx,
        findOperation,
        documentUpdater,
        false,
        false,
        upsert,
        shredder,
        DocumentProjector.includeAllProjector(),
        1,
        operationsConfig.lwt().retries());
  }

  private FindOperation getFindOperation(
      CommandContext<CollectionSchemaObject> ctx, UpdateOneCommand command) {
    LogicalExpression logicalExpression = resolve(ctx, command);

    final SortClause sortClause = command.sortClause();
    // validate sort path
    if (sortClause != null) {
      sortClause.validate(ctx);
    }

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    var indexUsage = ctx.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;
    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        logicalExpression,
        indexUsage);
    if (vector != null) {
      return FindOperation.vsearchSingle(
          ctx,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          false);
    }

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindOperation.sortedSingle(
          ctx,
          logicalExpression,
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
      return FindOperation.unsortedSingle(
          ctx,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          false);
    }
  }
}
