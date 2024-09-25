package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
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
    //    WritableShreddedDocument shreddedDocument =
    //        documentShredder.shred(
    //            command.document(),
    //            null,
    //            ctx.schemaObject().indexingProjector(),
    //            ctx.commandName(),
    //            ctx.schemaObject(),
    //            null);

    var builder =
        new CollectionInsertAttemptBuilder(ctx.schemaObject(), documentShredder, ctx.commandName());

    var attemps = List.of(builder.build(command.document()));
    return new InsertCollectionOperation(ctx, attemps, false, false, false);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, InsertOneCommand command) {

    var builder =
        new TableInsertAttemptBuilder(
            rowShredder,
            new WriteableTableRowBuilder(ctx.schemaObject(), JSONCodecRegistries.DEFAULT_REGISTRY));

    var attempts = new OperationAttemptContainer<>(builder.build(command.document()));

    InsertAttemptPage.Builder<TableSchemaObject> pageBuilder =
        InsertAttemptPage.<TableSchemaObject>builder()
            .returnDocumentResponses(false) // always false for single document insert
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GeneralOperation<>(ctx, new TableDriverExceptionHandler(), attempts, pageBuilder);
  }
}
