package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EstimatedDocumentCountCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.EstimatedDocumentCountCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
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
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, EstimatedDocumentCountCommand command) {
    return new EstimatedDocumentCountCollectionOperation(ctx);
  }
}
