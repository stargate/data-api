package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.InsertOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.tables.InsertTableOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.tables.TableInsertAttempt;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link InsertOneCommand}. */
@ApplicationScoped
public class InsertOneCommandResolver implements CommandResolver<InsertOneCommand> {

  private final Shredder documentShredder;
  private final RowShredder rowShredder;

  @Inject
  public InsertOneCommandResolver(Shredder documentShredder, RowShredder rowShredder) {
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
    return InsertOperation.create(ctx, shreddedDocument);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, InsertOneCommand command) {

    return new InsertTableOperation(
        ctx, TableInsertAttempt.create(rowShredder, command.document()));
  }
}
