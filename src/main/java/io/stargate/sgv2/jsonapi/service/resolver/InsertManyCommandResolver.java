package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingOperationFactory;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertDBTask;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueContainerFactory;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link InsertManyCommand}. */
@ApplicationScoped
public class InsertManyCommandResolver implements CommandResolver<InsertManyCommand> {

  private final DocumentShredder documentShredder;

  @Inject
  public InsertManyCommandResolver(DocumentShredder documentShredder) {
    this.documentShredder = documentShredder;
  }

  @Override
  public Class<InsertManyCommand> getCommandClass() {
    return InsertManyCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, InsertManyCommand command) {

    final InsertManyCommand.Options options = command.options();
    final boolean ordered = (null != options) && options.ordered();
    final boolean returnDocumentResponses = (null != options) && options.returnDocumentResponses();

    var builder =
        new CollectionInsertAttemptBuilder(ctx.schemaObject(), documentShredder, ctx.commandName());
    var attempts = command.documents().stream().map(builder::build).toList();

    return new InsertCollectionOperation(ctx, attempts, ordered, false, returnDocumentResponses);
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, InsertManyCommand command) {

    // TODO: move the default for ordered to a constant and use in the API
    var tasksAndDeferrables =
        TableInsertDBTask.builder(commandContext)
            .withOrdered(
                ApiOptionUtils.getOrDefault(
                    command.options(), InsertManyCommand.Options::ordered, false))
            .withReturnDocumentResponses(
                ApiOptionUtils.getOrDefault(
                    command.options(), InsertManyCommand.Options::returnDocumentResponses, false))
            .withJsonNamedValueFactory(
                new JsonNamedValueContainerFactory(
                    commandContext.schemaObject(), JsonNodeDecoder.DEFAULT))
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new)
            .build(command.documents());

    return EmbeddingOperationFactory.createOperation(commandContext, tasksAndDeferrables);
  }
}
