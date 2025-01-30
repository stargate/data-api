package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateTableExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CreateTableCommandResolver implements CommandResolver<CreateTableCommand> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableCommandResolver.class);

  @Inject ObjectMapper objectMapper;
  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, CreateTableCommand command) {

    var tableName = cqlIdentifierFromUserInput(command.name());

    boolean ifNotExists =
        Optional.ofNullable(command.options())
            .map(CreateTableCommand.Options::ifNotExists)
            .orElse(false);

    // TODO: AARON: this is where the bad user column types like list of map will be caught and
    // thrown
    // TODO: this code is also is alter table, remove the duplication

    var apiTableDef =
        ApiTableDef.FROM_TABLE_DESC_FACTORY.create(
            command.name(), command.definition(), validateVectorize);

    var customProperties =
        TableExtensions.createCustomProperties(
            apiTableDef.allColumns().getVectorizeDefs(), objectMapper);

    var attempt =
        new CreateTableAttemptBuilder(0, ctx.schemaObject())
            .retryDelayMillis(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis())
            .maxRetries(ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries())
            .tableDef(apiTableDef)
            .ifNotExists(ifNotExists)
            .customProperties(customProperties)
            .build();
    var attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        SchemaAttemptPage.<KeyspaceSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(
        attempts,
        pageBuilder,
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            CreateTableExceptionHandler::new, tableName));
  }

  @Override
  public Class<CreateTableCommand> getCommandClass() {
    return CreateTableCommand.class;
  }
}
