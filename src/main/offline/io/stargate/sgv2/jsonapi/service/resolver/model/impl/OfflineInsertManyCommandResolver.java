package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link OfflineInsertManyCommand}. */
@ApplicationScoped
public class OfflineInsertManyCommandResolver implements CommandResolver<OfflineInsertManyCommand> {
  private final OperationsConfig operationsConfig;
  private final DocumentShredder documentShredder;

  @Inject
  public OfflineInsertManyCommandResolver(
      DocumentShredder documentShredder, OperationsConfig operationsConfig) {
    this.documentShredder = documentShredder;
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<OfflineInsertManyCommand> getCommandClass() {
    return OfflineInsertManyCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, OfflineInsertManyCommand command) {
    if (command.documents().size()
        > operationsConfig.offlineModeConfig().maxDocumentInsertCount()) {
      throw new IllegalArgumentException("Exceeded max document insert count");
    }
    final List<WritableShreddedDocument> shreddedDocuments =
        command.documents().stream().map(doc -> documentShredder.shred(ctx, doc, null)).toList();

    // Offline insert is always ordered
    final boolean ordered = true;
    // and no need to return document positions
    final boolean returnDocumentResponses = false;

    return InsertCollectionOperation.create(
        ctx, shreddedDocuments, ordered, true, returnDocumentResponses);
  }
}
