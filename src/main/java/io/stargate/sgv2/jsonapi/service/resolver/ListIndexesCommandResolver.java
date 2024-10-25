package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListIndexesCommand;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.GenericOperation;
import io.stargate.sgv2.jsonapi.service.operation.ListIndexesAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.operation.MetadataAttemptPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptContainer;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/** Command resolver for the {@link ListIndexesCommand}. */
@ApplicationScoped
public class ListIndexesCommandResolver implements CommandResolver<ListIndexesCommand> {
  /** {@inheritDoc} */
  @Override
  public Class<ListIndexesCommand> getCommandClass() {
    return ListIndexesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, ListIndexesCommand command) {

    boolean explain = command.options() != null && command.options().explain();

    var attempt = new ListIndexesAttemptBuilder(ctx.schemaObject()).build();
    var attempts = new OperationAttemptContainer<>(List.of(attempt));

    var pageBuilder =
        MetadataAttemptPage.builder()
            .showSchema(explain)
            .usingCommandStatus(CommandStatus.EXISTING_INDEXES)
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation(attempts, pageBuilder, new TableDriverExceptionHandler());
  }
}
