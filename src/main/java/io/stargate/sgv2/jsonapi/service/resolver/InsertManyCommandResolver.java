package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.WriteableTableRowBuilder;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

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

    var builder =
        new CollectionInsertAttemptBuilder(ctx.schemaObject(), documentShredder, ctx.commandName());
    var attempts = command.documents().stream().map(builder::build).toList();

    return new InsertCollectionOperation(ctx, attempts, ordered, false, returnDocumentResponses);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, InsertManyCommand command) {

    final InsertManyCommand.Options options = command.options();
    final boolean ordered = (null != options) && options.ordered();
    final boolean returnDocumentResponses = (null != options) && options.returnDocumentResponses();

    var builder =
        new TableInsertAttemptBuilder(
            rowShredder,
            new WriteableTableRowBuilder(ctx.schemaObject(), JSONCodecRegistries.DEFAULT_REGISTRY));

    OperationAttemptContainer<TableSchemaObject, InsertAttempt<TableSchemaObject>> attempts =
        new OperationAttemptContainer<>(ordered);
    attempts.addAll(command.documents().stream().map(builder::build).toList());

    var pageBuilder =
        InsertAttemptPage.<TableSchemaObject>builder()
            .returnDocumentResponses(returnDocumentResponses)
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled());

    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }
}
