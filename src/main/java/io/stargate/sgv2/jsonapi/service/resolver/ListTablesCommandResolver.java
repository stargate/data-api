package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListTablesCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Command resolver for the {@link ListTablesCommand}. */
@ApplicationScoped
public class ListTablesCommandResolver implements CommandResolver<ListTablesCommand> {
  private final ObjectMapper objectMapper;
  private final CQLSessionCache cqlSessionCache;

  @Inject
  public ListTablesCommandResolver(ObjectMapper objectMapper, CQLSessionCache cqlSessionCache) {
    this.objectMapper = objectMapper;
    this.cqlSessionCache = cqlSessionCache;
  }

  /** {@inheritDoc} */
  @Override
  public Class<ListTablesCommand> getCommandClass() {
    return ListTablesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, ListTablesCommand command) {

    boolean explain = command.options() != null ? command.options().explain() : false;

    MetadataAttempt<KeyspaceSchemaObject> attempt =
        new ListTablesAttemptBuilder(ctx.schemaObject()).build();
    OperationAttemptContainer<KeyspaceSchemaObject, MetadataAttempt<KeyspaceSchemaObject>> attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        MetadataAttemptPage.<KeyspaceSchemaObject>builder()
            .showSchema(explain)
            .usingCommandStatus(CommandStatus.EXISTING_TABLES)
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(attempts, pageBuilder, KeyspaceDriverExceptionHandler::new);
  }
}
