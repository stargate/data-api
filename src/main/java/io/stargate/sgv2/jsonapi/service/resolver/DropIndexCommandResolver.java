package io.stargate.sgv2.jsonapi.service.resolver;

import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Resolver for the {@link DropIndexCommand}. */
@ApplicationScoped
public class DropIndexCommandResolver implements CommandResolver<DropIndexCommand> {
  @Override
  public Class<DropIndexCommand> getCommandClass() {
    return DropIndexCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, DropIndexCommand command) {
    final SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy =
        new SchemaAttempt.SchemaRetryPolicy(
            ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
            Duration.ofMillis(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis()));
    final boolean ifExists =
        Optional.ofNullable(command.options())
            .map(DropIndexCommand.Options::ifExists)
            .orElse(false);
    CQLOption<Drop> cqlOption = null;
    if (ifExists) {
      cqlOption = CQLOption.ForDrop.ifExists();
    }
    var attempt =
        new DropIndexAttemptBuilder(ctx.schemaObject(), command.name(), schemaRetryPolicy)
            .withIfExists(cqlOption)
            .build();
    var attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        SchemaAttemptPage.<KeyspaceSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled());

    return new GenericOperation<>(
        attempts, pageBuilder, new KeyspaceDriverExceptionHandler(command));
  }
}
