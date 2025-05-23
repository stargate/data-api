package io.stargate.sgv2.jsonapi.service.resolver;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CountCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link CountDocumentsCommand } */
@ApplicationScoped
public class CountDocumentsCommandResolver implements CommandResolver<CountDocumentsCommand> {

  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final CollectionFilterResolver<CountDocumentsCommand> collectionFilterResolver;

  @Inject
  public CountDocumentsCommandResolver(
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.operationsConfig = operationsConfig;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<CountDocumentsCommand> getCommandClass() {
    return CountDocumentsCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> commandContext, CountDocumentsCommand command) {

    DBLogicalExpression dbLogicalExpression =
        collectionFilterResolver.resolve(commandContext, command).target();
    addToMetrics(
        meterRegistry,
        commandContext.requestContext(),
        jsonApiMetricsConfig,
        command,
        dbLogicalExpression,
        commandContext.schemaObject().newIndexUsage());

    return new CountCollectionOperation(
        commandContext,
        dbLogicalExpression,
        operationsConfig.defaultCountPageSize(),
        operationsConfig.maxCountLimit());
  }
}
