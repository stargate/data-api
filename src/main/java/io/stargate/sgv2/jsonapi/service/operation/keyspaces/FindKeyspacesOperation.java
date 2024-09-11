package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Operation that list all available keyspaces into the {@link CommandStatus#EXISTING_KEYSPACES}
 * command status. OR namespaces into the {@link CommandStatus#EXISTING_NAMESPACES} command status.
 *
 * <p>Note, both FindKeyspacesCommandResolver and FindNamespaceCommandResolver resolve to
 * FindKeyspaceOperation
 */
public class FindKeyspacesOperation implements Operation {

  Command fromCommand;

  /**
   * Construct FindKeyspacesOperation, and specify it is from command
   * FindKeyspacesCommand/FindNamespacesCommand
   *
   * @param fromCommand
   */
  public FindKeyspacesOperation(Command fromCommand) {
    this.fromCommand = fromCommand;
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    return Uni.createFrom()
        .item(
            () -> {
              // get all existing keyspaces
              List<String> keyspacesList =
                  queryExecutor
                      .getCqlSessionCache()
                      .getSession(dataApiRequestInfo)
                      .getMetadata()
                      .getKeyspaces()
                      .keySet()
                      .stream()
                      .map(CqlIdentifier::asInternal)
                      .toList();
              return new Result(keyspacesList, fromCommand);
            });
  }

  // simple result wrapper
  private record Result(List<String> keyspaces, Command fromCommand)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses;
      if (fromCommand instanceof FindNamespacesCommand) {
        statuses = Map.of(CommandStatus.EXISTING_NAMESPACES, keyspaces);
      } else {
        statuses = Map.of(CommandStatus.EXISTING_KEYSPACES, keyspaces);
      }
      return new CommandResult(statuses);
    }
  }
}
