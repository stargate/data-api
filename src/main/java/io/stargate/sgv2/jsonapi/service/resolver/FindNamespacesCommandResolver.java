package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.FindKeyspacesOperation;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Command resolver for {@link FindNamespacesCommand}. Resolve a FindNamespacesCommand to a {@link
 * FindKeyspacesOperation}
 */
@ApplicationScoped
public class FindNamespacesCommandResolver extends FindNamespacesKeyspacesCommandResolver<FindNamespacesCommand> {

  public FindNamespacesCommandResolver() {}

  /** {@inheritDoc} */
  @Override
  public Class<FindNamespacesCommand> getCommandClass() {
    return FindNamespacesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, FindNamespacesCommand command) {
    return new FindKeyspacesOperation(false);
  }
}
