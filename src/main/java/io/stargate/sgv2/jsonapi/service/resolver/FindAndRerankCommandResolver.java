package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link FindAndRerankCommand } */
@ApplicationScoped
public class FindAndRerankCommandResolver implements CommandResolver<FindAndRerankCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;
  private final FindCommandResolver findCommandResolver;

  @Inject
  public FindAndRerankCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      FindCommandResolver findCommandResolver) {

    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.findCommandResolver = findCommandResolver;
  }

  @Override
  public Class<FindAndRerankCommand> getCommandClass() {
    return FindAndRerankCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> commandContext, FindAndRerankCommand command) {

    // TODO: add to metrics

    boolean isRerankingEnabledForAPI =
        commandContext.apiFeatures().isFeatureEnabled(ApiFeature.RERANKING);
    if (!isRerankingEnabledForAPI) {
      throw SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.get();
    }

    return new FindAndRerankOperationBuilder(commandContext)
        .withCommand(command)
        .withFindCommandResolver(findCommandResolver)
        .build();
  }
}
