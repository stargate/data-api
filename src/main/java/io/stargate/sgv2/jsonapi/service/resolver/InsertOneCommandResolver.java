package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.InsertTableOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertAttempt;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link InsertOneCommand}. */
@ApplicationScoped
public class InsertOneCommandResolver implements CommandResolver<InsertOneCommand> {

  private final DocumentShredder documentShredder;
  private final RowShredder rowShredder;

  @Inject
  public InsertOneCommandResolver(DocumentShredder documentShredder, RowShredder rowShredder) {
    this.documentShredder = documentShredder;
    this.rowShredder = rowShredder;
  }

  @Override
  public Class<InsertOneCommand> getCommandClass() {
    return InsertOneCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, InsertOneCommand command) {
    WritableShreddedDocument shreddedDocument =
        documentShredder.shred(
            command.document(),
            null,
            ctx.schemaObject().indexingProjector(),
            command.getClass().getSimpleName(),
            ctx.schemaObject(),
            null);
    return InsertCollectionOperation.create(ctx, shreddedDocument);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, InsertOneCommand command) {

    return new InsertTableOperation(
        ctx, TableInsertAttempt.create(rowShredder, ctx.schemaObject(), command.document()));
  }
}
