package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
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
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link InsertManyCommand}. */
@ApplicationScoped
public class InsertManyCommandResolver implements CommandResolver<InsertManyCommand> {

  private final DocumentShredder documentShredder;
  private final JsonNamedValueFactory rowShredder;

  @Inject
  public InsertManyCommandResolver(
      DocumentShredder documentShredder, JsonNamedValueFactory rowShredder) {
    this.documentShredder = documentShredder;
    this.rowShredder = rowShredder;
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

    TableInsertDBTaskBuilder taskBuilder =
        TableInsertDBTask.builder(commandContext.schemaObject())
            .withRowShredder(rowShredder)
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    // TODO: move the default for ordered to a constant and use in the API
    var taskGroup =
        new TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject>(
            ApiOptionUtils.getOrDefault(
                command.options(), InsertManyCommand.Options::ordered, false));
    taskGroup.addAll(command.documents().stream().map(taskBuilder::build).toList());

    var accumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(
                ApiOptionUtils.getOrDefault(
                    command.options(), InsertManyCommand.Options::returnDocumentResponses, false));

    return new TaskOperation<>(taskGroup, accumulator);
  }
}
