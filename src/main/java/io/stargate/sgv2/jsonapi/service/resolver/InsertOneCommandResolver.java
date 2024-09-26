package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tables.InsertTableOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.WriteableTableRowBuilder;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

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
            ctx.commandName(),
            ctx.schemaObject(),
            null);
    return InsertCollectionOperation.create(ctx, shreddedDocument);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, InsertOneCommand command) {

    var builder =
        new TableInsertAttemptBuilder(
            rowShredder,
            new WriteableTableRowBuilder(ctx.schemaObject(), JSONCodecRegistries.DEFAULT_REGISTRY));
    var attempts = List.of(builder.build(command.document()));

    return new InsertTableOperation(ctx, new TableDriverExceptionHandler(), attempts);
  }
}
