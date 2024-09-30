package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttempt;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tables.InsertTableOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.WriteableTableRowBuilder;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Resolves the {@link InsertManyCommand}. */
@ApplicationScoped
public class InsertManyCommandResolver implements CommandResolver<InsertManyCommand> {

  private final DocumentShredder documentShredder;
  private final RowShredder rowShredder;

  @Inject
  public InsertManyCommandResolver(DocumentShredder documentShredder, RowShredder rowShredder) {
    this.documentShredder = documentShredder;
    this.rowShredder = rowShredder;
  }

  @Override
  public Class<InsertManyCommand> getCommandClass() {
    return InsertManyCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, InsertManyCommand command) {

    final InsertManyCommand.Options options = command.options();
    final boolean ordered = (null != options) && options.ordered();
    final boolean returnDocumentResponses = (null != options) && options.returnDocumentResponses();
    final List<JsonNode> inputDocs = command.documents();
    final int docCount = inputDocs.size();

    final List<CollectionInsertAttempt> insertions = new ArrayList<>(docCount);
    for (int pos = 0; pos < docCount; ++pos) {
      CollectionInsertAttempt attempt;
      // Since exception thrown will prevent returning anything, need to instead pass a
      // reference for Shredder to populate with the document id as soon as it knows it
      // (there is at least one case fail occurs before it knows the id)
      AtomicReference<DocumentId> idRef = new AtomicReference<>();
      try {
        final WritableShreddedDocument shredded =
            documentShredder.shred(ctx, inputDocs.get(pos), null, idRef);
        attempt = CollectionInsertAttempt.from(pos, shredded);
      } catch (Exception e) {
        // TODO: need a base Shredding exception to catch
        attempt = new CollectionInsertAttempt(pos, idRef.get(), e);
      }
      insertions.add(attempt);
    }
    return new InsertCollectionOperation(ctx, insertions, ordered, false, returnDocumentResponses);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, InsertManyCommand command) {

    var builder =
        new TableInsertAttemptBuilder(
            rowShredder,
            new WriteableTableRowBuilder(ctx.schemaObject(), JSONCodecRegistries.DEFAULT_REGISTRY));
    var attempts = command.documents().stream().map(builder::build).toList();

    return new InsertTableOperation(ctx, new TableDriverExceptionHandler(), attempts);
  }
}
