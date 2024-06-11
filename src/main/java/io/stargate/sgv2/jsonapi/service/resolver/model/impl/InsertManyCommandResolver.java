package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link InsertManyCommand}. */
@ApplicationScoped
public class InsertManyCommandResolver implements CommandResolver<InsertManyCommand> {

  private final Shredder shredder;

  @Inject
  public InsertManyCommandResolver(Shredder shredder) {
    this.shredder = shredder;
  }

  @Override
  public Class<InsertManyCommand> getCommandClass() {
    return InsertManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, InsertManyCommand command) {
    final List<WritableShreddedDocument> shreddedDocuments =
        command.documents().stream().map(doc -> shredder.shred(ctx, doc, null)).toList();

    // resolve ordered
    InsertManyCommand.Options options = command.options();

    boolean ordered = null != options && options.ordered();
    boolean returnDocumentPositions = null != options && options.returnDocumentPositions();

    return new InsertOperation(ctx, shreddedDocuments, ordered, false, returnDocumentPositions);
  }
}
