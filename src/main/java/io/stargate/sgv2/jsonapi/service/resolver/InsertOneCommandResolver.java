package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingOperationFactory;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertDBTask;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueContainerFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link InsertOneCommand}. */
@ApplicationScoped
public class InsertOneCommandResolver implements CommandResolver<InsertOneCommand> {

  private final DocumentShredder documentShredder;

  @Inject
  public InsertOneCommandResolver(DocumentShredder documentShredder) {
    this.documentShredder = documentShredder;
  }

  @Override
  public Class<InsertOneCommand> getCommandClass() {
    return InsertOneCommand.class;
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, InsertOneCommand command) {

    var builder =
        new CollectionInsertAttemptBuilder(ctx.schemaObject(), documentShredder, ctx.commandName());

    var attemps = List.of(builder.build(command.document()));
    return new InsertCollectionOperation(ctx, attemps, false, false, false);
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, InsertOneCommand command) {

    var tasksAndDeferrables =
        TableInsertDBTask.builder(commandContext)
            .withOrdered(false)
            .withReturnDocumentResponses(false) // never for insertOne
            .withJsonNamedValueFactory(
                new JsonNamedValueContainerFactory(
                    commandContext.schemaObject(), JsonNodeDecoder.DEFAULT))
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new)
            .build(List.of(command.document()));

    return EmbeddingOperationFactory.createOperation(commandContext, tasksAndDeferrables);
  }
}
