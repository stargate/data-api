package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.TruncateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Resolves the {@link DeleteManyCommand } DeleteOne command implements Filterable to identify the
 * records to delete based on the filter condition and deletes it.
 */
@ApplicationScoped
public class DeleteManyCommandResolver extends FilterableResolver<DeleteManyCommand>
    implements CommandResolver<DeleteManyCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public DeleteManyCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.operationsConfig = operationsConfig;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, DeleteManyCommand command) {
    // If there is no filter or filter is empty, use Truncate operation instead of Delete
    if (command.filterClause() == null || command.filterClause().logicalExpression().isEmpty()) {
      return new TruncateCollectionOperation(commandContext);
    }
    final FindOperation findOperation = getFindOperation(commandContext, command);
    return DeleteOperation.delete(
        commandContext,
        findOperation,
        operationsConfig.maxDocumentDeleteCount(),
        operationsConfig.lwt().retries());
  }

  @Override
  public Class<DeleteManyCommand> getCommandClass() {
    return DeleteManyCommand.class;
  }

  private FindOperation getFindOperation(CommandContext commandContext, DeleteManyCommand command) {
    LogicalExpression logicalExpression = resolve(commandContext, command);
    // Read One extra document than delete limit so return moreData flag
    addToMetrics(
        meterRegistry, dataApiRequestInfo, jsonApiMetricsConfig, command, logicalExpression, false);
    return FindOperation.unsorted(
        commandContext,
        logicalExpression,
        DocumentProjector.includeAllProjector(),
        null,
        operationsConfig.maxDocumentDeleteCount() + 1,
        operationsConfig.defaultPageSize(),
        ReadType.KEY,
        objectMapper,
        false);
  }
}
