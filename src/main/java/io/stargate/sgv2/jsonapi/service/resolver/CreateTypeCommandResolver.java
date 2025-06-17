package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTypeCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtDef;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CreateTypeCommandResolver implements CommandResolver<CreateTypeCommand> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTypeCommandResolver.class);

  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, CreateTypeCommand command) {

    // get the task builder
    var taskBuilder =
        CreateTypeDBTask.builder(commandContext.schemaObject())
            .withSchemaRetryPolicy(
                new SchemaDBTask.SchemaRetryPolicy(
                    commandContext
                        .config()
                        .get(OperationsConfig.class)
                        .databaseConfig()
                        .ddlRetries(),
                    Duration.ofMillis(
                        commandContext
                            .config()
                            .get(OperationsConfig.class)
                            .databaseConfig()
                            .ddlRetryDelayMillis())));

    // validate the UDT name
    var udtName = validateSchemaName(command.name(), NamingRules.UDT);

    // set the ifNotExists option if provided
    taskBuilder.ifNotExists(
        ApiOptionUtils.getOrDefault(
            command.options(), CreateTypeCommand.Options::ifNotExists, false));

    // create the ApiUdtDef from the command definition
    var apiUdtDef =
        ApiUdtDef.FROM_TYPE_DESC_FACTORY.create(udtName, command.definition(), validateVectorize);

    // append the task with the UDT definition and exception handler.
    taskBuilder
        .udtDef(apiUdtDef)
        .withExceptionHandlerFactory(
            DefaultDriverExceptionHandler.Factory.withIdentifier(
                CreateTypeExceptionHandler::new, cqlIdentifierFromUserInput(udtName)));

    var taskGroup = new TaskGroup<>(taskBuilder.build());
    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(CreateTypeDBTask.class, commandContext));
  }

  @Override
  public Class<CreateTypeCommand> getCommandClass() {
    return CreateTypeCommand.class;
  }
}
