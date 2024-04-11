package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link OfflineInsertManyCommand}. */
@ApplicationScoped
public class OfflineInsertManyCommandResolver implements CommandResolver<OfflineInsertManyCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public OfflineInsertManyCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<OfflineInsertManyCommand> getCommandClass() {
    return OfflineInsertManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineInsertManyCommand command) {
    final IndexingProjector projection = ctx.indexingProjector();
    final List<WritableShreddedDocument> shreddedDocuments =
        command.documents().stream().map(doc -> shredder.shred(ctx, doc, null)).toList();

    // Offline insert is always ordered
    boolean ordered = true;

    return new InsertOperation(ctx, shreddedDocuments, ordered);
  }
}
