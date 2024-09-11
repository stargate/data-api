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

  /**
   * useKeyspaceNaming will be false, if this operation is created by deprecated FindNamespacesCommand
   */
  private final boolean useKeyspaceNaming;

  /**
   * Construct FindKeyspacesOperation, and specify it is from command
   * FindKeyspacesCommand/FindNamespacesCommand by using useKeyspaceNaming boolean
   *
   * @param useKeyspaceNaming a boolean value indicated use keyspace or not
   */
  public FindKeyspacesOperation(boolean useKeyspaceNaming) {
    this.useKeyspaceNaming = useKeyspaceNaming;
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
              return new Result(keyspacesList, useKeyspaceNaming);
            });
  }

  // simple result wrapper
  private record Result(List<String> keyspaces, boolean useKeyspaceNaming)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses = useKeyspaceNaming ? Map.of(CommandStatus.EXISTING_KEYSPACES, keyspaces) : Map.of(CommandStatus.EXISTING_NAMESPACES, keyspaces);
      return new CommandResult(statuses);
    }
  }
}
