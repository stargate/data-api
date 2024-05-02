package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link OfflineInsertManyCommand}. */
@ApplicationScoped
public class OfflineInsertManyCommandResolver implements CommandResolver<OfflineInsertManyCommand> {
  private final OperationsConfig operationsConfig;
  private final Shredder shredder;

  @Inject
  public OfflineInsertManyCommandResolver(Shredder shredder, OperationsConfig operationsConfig) {
    this.shredder = shredder;
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<OfflineInsertManyCommand> getCommandClass() {
    return OfflineInsertManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineInsertManyCommand command) {
    if (command.documents().size()
        > operationsConfig.offlineModeConfig().maxDocumentInsertCount()) {
      throw new IllegalArgumentException("Exceeded max document insert count");
    }
    final List<WritableShreddedDocument> shreddedDocuments =
        command.documents().stream().map(doc -> shredder.shred(ctx, doc, null)).toList();

    // Offline insert is always ordered
    boolean ordered = true;

    return new InsertOperation(ctx, shreddedDocuments, ordered, true);
  }
}
