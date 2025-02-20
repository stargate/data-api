package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertDBTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link InsertOneCommand}. */
@ApplicationScoped
public class InsertOneCommandResolver implements CommandResolver<InsertOneCommand> {

  private final DocumentShredder documentShredder;
  private final JsonNamedValueFactory rowShredder;

  @Inject
  public InsertOneCommandResolver(DocumentShredder documentShredder, JsonNamedValueFactory rowShredder) {
    this.documentShredder = documentShredder;
    this.rowShredder = rowShredder;
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

    TableInsertDBTaskBuilder taskBuilder =
        TableInsertDBTask.builder(commandContext.schemaObject())
            .withRowShredder(rowShredder)
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    var taskGroup =
        new TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject>(
            taskBuilder.build(command.document()));

    var accumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(false); // never for insertOne

    return new TaskOperation<>(taskGroup, accumulator);
  }
}
