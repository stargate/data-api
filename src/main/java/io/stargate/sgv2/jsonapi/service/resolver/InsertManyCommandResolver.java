package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionInsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.collections.InsertCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableInsertDBTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.WriteableTableRowBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

    var builder =
        new CollectionInsertAttemptBuilder(ctx.schemaObject(), documentShredder, ctx.commandName());
    var attempts = command.documents().stream().map(builder::build).toList();

    return new InsertCollectionOperation(ctx, attempts, ordered, false, returnDocumentResponses);
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, InsertManyCommand command) {

    final InsertManyCommand.Options options = command.options();
    final boolean ordered = (null != options) && options.ordered();
    final boolean returnDocumentResponses = (null != options) && options.returnDocumentResponses();

    TableInsertDBTaskBuilder taskBuilder =
        new TableInsertDBTaskBuilder(commandContext.schemaObject())
            .withRowShredder(rowShredder)
            .withWriteableTableRowBuilder(
                new WriteableTableRowBuilder(
                    commandContext.schemaObject(), JSONCodecRegistries.DEFAULT_REGISTRY))
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    TaskGroup<InsertDBTask<TableSchemaObject>, TableSchemaObject> taskGroup =
        new TaskGroup<>(ordered);
    taskGroup.addAll(command.documents().stream().map(taskBuilder::build).toList());

    var accumulator =
        InsertDBTaskPage.accumulator(commandContext)
            .returnDocumentResponses(returnDocumentResponses);

    return new TaskOperation<>(taskGroup, accumulator);

    TableInsertDBTaskBuilder builder =
        new TableInsertDBTaskBuilder(commandContext.schemaObject())
            .withRowShredder(rowShredder)
            .withWriteableTableRowBuilder(
                new WriteableTableRowBuilder(
                    commandContext.schemaObject(), JSONCodecRegistries.DEFAULT_REGISTRY))
            .withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    var tasks = new TaskGroup<>(builder.build(command.document()));
    InsertDBTaskPage.Accumulator<TableSchemaObject> accumulator =
        InsertDBTaskPage.accumulator(commandContext).returnDocumentResponses(false);

    return new TaskOperation<>(tasks, accumulator);
  }
}
