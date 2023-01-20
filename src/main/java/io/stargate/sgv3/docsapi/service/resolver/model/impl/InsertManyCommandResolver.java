package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv3.docsapi.service.resolver.model.CommandResolver;
import io.stargate.sgv3.docsapi.service.shredding.Shredder;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

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
        command.documents().stream().map(doc -> shredder.shred(doc)).collect(Collectors.toList());
    return new InsertOperation(ctx, shreddedDocuments);
  }
}
