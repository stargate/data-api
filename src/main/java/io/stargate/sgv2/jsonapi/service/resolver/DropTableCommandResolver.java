package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropTableAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.List;

/** Resolver for the {@link DropTableCommand}. */
@ApplicationScoped
public class DropTableCommandResolver implements CommandResolver<DropTableCommand> {
  @Override
  public Class<DropTableCommand> getCommandClass() {
    return DropTableCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, DropTableCommand command) {
    final DropTableCommand.Options options = command.options();
    final boolean ifExists = (options != null) && options.ifExists();
    final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy =
        new SchemaAttempt.SchemaRetryPolicy(
            ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
            Duration.ofMillis(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis()));
    CQLOption cqlOption = null;
    if (ifExists) {
      cqlOption = CQLOption.ForDrop.ifExists();
    }
    var attempt =
        new DropTableAttemptBuilder(ctx.schemaObject(), command.name(), schemaRetryPolicy)
            .withIfExists(cqlOption)
            .build();
    var attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        SchemaAttemptPage.<KeyspaceSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(
        attempts, pageBuilder, new KeyspaceDriverExceptionHandler(command));
  }
}
