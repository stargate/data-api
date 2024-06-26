package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link UpdateManyCommand } */
@ApplicationScoped
public class UpdateManyCommandResolver extends FilterableResolver<UpdateManyCommand>
    implements CommandResolver<UpdateManyCommand> {
  private final Shredder shredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final DataVectorizerService dataVectorizerService;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public UpdateManyCommandResolver(
      ObjectMapper objectMapper,
      Shredder shredder,
      OperationsConfig operationsConfig,
      DataVectorizerService dataVectorizerService,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.operationsConfig = operationsConfig;
    this.dataVectorizerService = dataVectorizerService;
    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public Class<UpdateManyCommand> getCommandClass() {
    return UpdateManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, UpdateManyCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.updateClause());

    // resolve upsert
    UpdateManyCommand.Options options = command.options();
    boolean upsert = options != null && options.upsert();

    // return op
    return new ReadAndUpdateOperation(
        commandContext,
        findOperation,
        documentUpdater,
        dataVectorizerService,
        false,
        false,
        upsert,
        shredder,
        DocumentProjector.includeAllProjector(),
        operationsConfig.maxDocumentUpdateCount(),
        operationsConfig.lwt().retries());
  }

  private FindOperation getFindOperation(CommandContext commandContext, UpdateManyCommand command) {
    LogicalExpression logicalExpression = resolve(commandContext, command);
    addToMetrics(
        meterRegistry, dataApiRequestInfo, jsonApiMetricsConfig, command, logicalExpression, false);
    return FindOperation.unsorted(
        commandContext,
        logicalExpression,
        DocumentProjector.includeAllProjector(),
        null != command.options() ? command.options().pageState() : null,
        Integer.MAX_VALUE,
        operationsConfig.defaultPageSize(),
        ReadType.DOCUMENT,
        objectMapper,
        false);
  }
}
