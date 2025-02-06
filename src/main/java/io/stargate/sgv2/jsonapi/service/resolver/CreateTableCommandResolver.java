package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CreateTableCommandResolver implements CommandResolver<CreateTableCommand> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableCommandResolver.class);

  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, CreateTableCommand command) {

    var taskBuilder = CreateTableDBTask.builder(commandContext.schemaObject())
            .withSchemaRetryPolicy(
                new SchemaDBTask.SchemaRetryPolicy(
                    commandContext.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
                    Duration.ofMillis(
                        commandContext
                            .getConfig(OperationsConfig.class)
                            .databaseConfig()
                            .ddlRetryDelayMillis())));

    taskBuilder.ifNotExists(
        ApiOptionUtils.getOrDefault(
            command.options(), CreateTableCommand.Options::ifNotExists, false));

    var apiTableDef =
        ApiTableDef.FROM_TABLE_DESC_FACTORY.create(
            command.name(), command.definition(), validateVectorize);
    // todo - move the custom properties building into the builder
    taskBuilder
        .tableDef(apiTableDef)
        .customProperties(
            TableExtensions.createCustomProperties(
                apiTableDef.allColumns().getVectorizeDefs(), objectMapper))
        .withExceptionHandlerFactory(
            DefaultDriverExceptionHandler.Factory.withIdentifier(
                CreateTableExceptionHandler::new, cqlIdentifierFromUserInput(command.name())));

    var taskGroup = new TaskGroup<>(taskBuilder.build());

    return new TaskOperation<>(taskGroup, SchemaDBTaskPage.accumulator(CreateTableDBTask.class, commandContext));
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }
}
