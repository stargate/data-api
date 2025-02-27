package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/** Resolver for the {@link DropIndexCommand}. */
@ApplicationScoped
public class DropIndexCommandResolver implements CommandResolver<DropIndexCommand> {

  private static final boolean IF_EXISTS_DEFAULT = false;

  @Override
  public Class<DropIndexCommand> getCommandClass() {
    return DropIndexCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, DropIndexCommand command) {

    var indexName = cqlIdentifierFromUserInput(command.name());
    // Check if the index exists, we check if columns exist before trying to drop them so do for
    // indexes as well

    var attemptBuilder =
        new DropIndexAttemptBuilder(ctx.schemaObject())
            .withIfExists(
                command.options() == null ? IF_EXISTS_DEFAULT : command.options().ifExists())
            .withIndexName(indexName);

    // TODO: there should be a central factory to build these
    attemptBuilder =
        attemptBuilder.withSchemaRetryPolicy(
            new SchemaAttempt.SchemaRetryPolicy(
                ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
                Duration.ofMillis(
                    ctx.getConfig(OperationsConfig.class).databaseConfig().ddlRetryDelayMillis())));

    var attempts = new OperationAttemptContainer<>(attemptBuilder.build());

    var pageBuilder =
        SchemaAttemptPage.<KeyspaceSchemaObject>builder()
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled());

    return new GenericOperation<>(
        attempts,
        pageBuilder,
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            DropIndexExceptionHandler::new, indexName));
  }

  private void checkIndexExists(KeyspaceSchemaObject schemaObject, CqlIdentifier indexName) {}
}
