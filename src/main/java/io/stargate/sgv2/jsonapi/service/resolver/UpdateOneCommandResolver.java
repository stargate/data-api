package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.TableUpdateResolver;
import io.stargate.sgv2.jsonapi.service.resolver.update.UpdateResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link UpdateOneCommand } */
@ApplicationScoped
public class UpdateOneCommandResolver implements CommandResolver<UpdateOneCommand> {
  private final DocumentShredder documentShredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final DataVectorizerService dataVectorizerService;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final FilterResolver<UpdateOneCommand, CollectionSchemaObject> collectionFilterResolver;
  private final FilterResolver<UpdateOneCommand, TableSchemaObject> tableFilterResolver;

  private final UpdateResolver<UpdateOneCommand, TableSchemaObject> tableUpdateResolver;

  @Inject
  public UpdateOneCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      DocumentShredder documentShredder,
      DataVectorizerService dataVectorizerService,
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
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
    this.tableUpdateResolver = new TableUpdateResolver<>(operationsConfig);
  }

  @Override
  public Class<UpdateOneCommand> getCommandClass() {
    return UpdateOneCommand.class;
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, UpdateOneCommand command) {

    // Sort clause is not supported for table updateOne command.
    if (command.sortClause() != null && !command.sortClause().isEmpty()) {
      throw SortException.Code.UNSUPPORTED_SORT_FOR_TABLE_UPDATE_COMMAND.get(
          errVars(commandContext.schemaObject(), map -> {}));
    }

    var taskBuilder = UpdateDBTask.builder(commandContext.schemaObject()).withUpdateOne(true);
    taskBuilder.withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    // need to update so we use WithWarnings correctly
    var where =
        TableWhereCQLClause.forUpdate(
            commandContext.schemaObject(),
            tableFilterResolver.resolve(commandContext, command).target());

    var taskGroup =
        new TaskGroup<>(
            taskBuilder.build(where, tableUpdateResolver.resolve(commandContext, command)));

    return new TaskOperation<>(taskGroup, UpdateAttemptPage.accumulator(commandContext));
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, UpdateOneCommand command) {
    FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());

    // resolve upsert
    UpdateOneCommand.Options options = command.options();
    boolean upsert = options != null && options.upsert();

    // return op
    return new ReadAndUpdateCollectionOperation(
        ctx,
        findCollectionOperation,
        documentUpdater,
        dataVectorizerService,
        false,
        false,
        upsert,
        documentShredder,
        DocumentProjector.includeAllProjector(),
        1,
        operationsConfig.lwt().retries());
  }

  private FindCollectionOperation getFindOperation(
      CommandContext<CollectionSchemaObject> commandContext, UpdateOneCommand command) {

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
