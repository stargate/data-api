package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.provider.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneAndUpdateCommand } */
@ApplicationScoped
public class FindOneAndUpdateCommandResolver implements CommandResolver<FindOneAndUpdateCommand> {
  private final DocumentShredder documentShredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final CollectionFilterResolver<FindOneAndUpdateCommand> collectionFilterResolver;

  private final DataVectorizerService dataVectorizerService;

  @Inject
  public FindOneAndUpdateCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      DataVectorizerService dataVectorizerService,
      DocumentShredder documentShredder,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.documentShredder = documentShredder;
    this.operationsConfig = operationsConfig;
    this.dataVectorizerService = dataVectorizerService;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<FindOneAndUpdateCommand> getCommandClass() {
    return FindOneAndUpdateCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneAndUpdateCommand command) {
    FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);

    final DocumentProjector documentProjector = command.buildProjector();

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());

    // resolve options
    FindOneAndUpdateCommand.Options options = command.options();
    boolean returnUpdatedDocument =
        options != null && "after".equals(command.options().returnDocument());
    boolean upsert = command.options() != null && command.options().upsert();

    // return
    return new ReadAndUpdateCollectionOperation(
        ctx,
        findCollectionOperation,
        documentUpdater,
        dataVectorizerService,
        true,
        returnUpdatedDocument,
        upsert,
        documentShredder,
        documentProjector,
        1,
        operationsConfig.lwt().retries());
  }

  private FindCollectionOperation getFindOperation(
      CommandContext<CollectionSchemaObject> commandContext, FindOneAndUpdateCommand command) {

    var dbLogicalExpression = collectionFilterResolver.resolve(commandContext, command).target();

    final SortClause sortClause = command.sortClause();
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
          CollectionReadType.DOCUMENT,
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
          CollectionReadType.DOCUMENT,
          objectMapper,
          bm25Expr);
    }
    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sortedSingle(
          commandContext,
          dbLogicalExpression,
          // 24-Mar-2023, tatu: Since we update the document, need to avoid modifications on
          // read path:
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
          dbLogicalExpression,
          // 24-Mar-2023, tatu: Since we update the document, need to avoid modifications on
          // read path:
          DocumentProjector.includeAllProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          false);
    }
  }
}
