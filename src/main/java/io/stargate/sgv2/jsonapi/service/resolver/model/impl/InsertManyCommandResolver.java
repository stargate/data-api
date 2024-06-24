package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
    final InsertManyCommand.Options options = command.options();
    final boolean ordered = (null != options) && options.ordered();
    final boolean returnDocumentResponses = (null != options) && options.returnDocumentResponses();
    final List<JsonNode> inputDocs = command.documents();
    final int docCount = inputDocs.size();

    final List<InsertOperation.InsertAttempt> insertions = new ArrayList<>(docCount);
    for (int pos = 0; pos < docCount; ++pos) {
      InsertOperation.InsertAttempt attempt;
      // Since exception thrown will prevent returning anything, need to instead pass a
      // reference for Shredder to populate with the document id as soon as it knows it
      // (there is at least one case fail occurs before it knows the id)
      AtomicReference<DocumentId> idRef = new AtomicReference<>();
      try {
        final WritableShreddedDocument shredded =
            shredder.shred(ctx, inputDocs.get(pos), null, idRef);
        attempt = new InsertOperation.InsertAttempt(pos, shredded);
      } catch (Exception e) {
        attempt = new InsertOperation.InsertAttempt(pos, idRef.get(), e);
      }
      insertions.add(attempt);
    }
    return new InsertOperation(ctx, insertions, ordered, false, returnDocumentResponses);
  }
}
