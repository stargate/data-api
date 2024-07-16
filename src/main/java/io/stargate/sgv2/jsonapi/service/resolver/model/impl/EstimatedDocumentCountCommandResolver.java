package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EstimatedDocumentCountCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.EstimatedDocumentCountOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link CountDocumentsCommand } */
@ApplicationScoped
public class EstimatedDocumentCountCommandResolver
    implements CommandResolver<EstimatedDocumentCountCommand> {

  private final OperationsConfig operationsConfig;

  @Inject
  public EstimatedDocumentCountCommandResolver(OperationsConfig operationsConfig) {
    super();
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<EstimatedDocumentCountCommand> getCommandClass() {
    return EstimatedDocumentCountCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, EstimatedDocumentCountCommand command) {
    return new EstimatedDocumentCountOperation(ctx);
  }
}
