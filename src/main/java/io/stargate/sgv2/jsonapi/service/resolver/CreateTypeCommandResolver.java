package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

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
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
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

    // create the ApiUdtDef from the command definition
    // will validate name etc
    var apiUdtType =
        ApiUdtType.FROM_TYPE_DESC_FACTORY.create(
            command.name(), command.definition(), validateVectorize);

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
                            .ddlRetryDelayMillis())))
            .ifNotExists(
                getOrDefault(command.options(), CreateTypeCommand.Options::ifNotExists, false))
            .withExceptionHandlerFactory(
                DefaultDriverExceptionHandler.Factory.withIdentifier(
                    CreateTypeExceptionHandler::new, apiUdtType.udtName()))
            .withApiUdtType(apiUdtType);

    return new TaskOperation<>(
        new TaskGroup<>(taskBuilder.build()), SchemaDBTaskPage.accumulator(CreateTypeDBTask.class, commandContext));
  }

  @Override
  public Class<CreateTypeCommand> getCommandClass() {
    return CreateTypeCommand.class;
  }
}
