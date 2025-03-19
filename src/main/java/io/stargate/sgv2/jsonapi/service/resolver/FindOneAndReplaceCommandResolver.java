package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneAndReplaceCommand } */
@ApplicationScoped
public class FindOneAndReplaceCommandResolver implements CommandResolver<FindOneAndReplaceCommand> {
  private final DocumentShredder documentShredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final DataVectorizerService dataVectorizerService;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final CollectionFilterResolver<FindOneAndReplaceCommand> collectionFilterResolver;

  @Inject
  public FindOneAndReplaceCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      DocumentShredder shredder,
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
  public Class<FindOneAndReplaceCommand> getCommandClass() {
    return FindOneAndReplaceCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneAndReplaceCommand command) {
    // Add $vector and $vectorize replacement validation here
    if (command.replacementDocument().has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
        && command.replacementDocument().has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
      throw ErrorCodeV1.INVALID_USAGE_OF_VECTORIZE.toApiException();
    }

    //
    FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);

    final DocumentProjector documentProjector = command.buildProjector();
    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.replacementDocument());

    // resolve options
    FindOneAndReplaceCommand.Options options = command.options();
    boolean returnUpdatedDocument =
        options != null && "after".equals(command.options().returnDocument());
    boolean upsert = options != null && options.upsert();
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
      CommandContext<CollectionSchemaObject> commandContext, FindOneAndReplaceCommand command) {

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
      throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
          "BM25 search is not yet supported for this command");
      // Likely implementation of [data-api#1939] to support BM25 sort
      /*
      return FindCollectionOperation.bm25Single(
              commandContext,
              dbLogicalExpression,
              DocumentProjector.includeAllProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              bm25Expr);
       */
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
    } else {
      return FindCollectionOperation.unsortedSingle(
          commandContext,
          dbLogicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          false);
    }
  }
}
