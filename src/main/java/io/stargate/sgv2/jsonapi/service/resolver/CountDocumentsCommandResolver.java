package io.stargate.sgv2.jsonapi.service.resolver;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CountCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterLogicalExpression;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link CountDocumentsCommand } */
@ApplicationScoped
public class CountDocumentsCommandResolver implements CommandResolver<CountDocumentsCommand> {

  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final CollectionFilterResolver<CountDocumentsCommand> collectionFilterResolver;

  @Inject
  public CountDocumentsCommandResolver(
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.operationsConfig = operationsConfig;
    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<CountDocumentsCommand> getCommandClass() {
    return CountDocumentsCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, CountDocumentsCommand command) {
    DBFilterLogicalExpression dbFilterLogicalExpression =
        collectionFilterResolver.resolve(ctx, command);
    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        dbFilterLogicalExpression,
        ctx.schemaObject().newIndexUsage());

    return new CountCollectionOperation(
        ctx,
        dbFilterLogicalExpression,
        operationsConfig.defaultCountPageSize(),
        operationsConfig.maxCountLimit());
  }
}
