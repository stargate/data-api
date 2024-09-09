package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.FindNamespacesOperation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Command resolver for {@link FindNamespacesCommand}. Resolve a FindNamespacesCommand to a {@link
 * FindNamespacesOperation}
 */
@ApplicationScoped
public class FindNamespacesCommandResolver implements CommandResolver<FindNamespacesCommand> {

  @Inject CQLSessionCache cqlSessionCache;

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
    // Instead of using new FindKeyspacesOperation, still using FindNamespacesOperation.
    // This is because there is namespaces as JsonProperty returned in FindNamespacesCommand
    // Support for backwards-compatibility
    return new FindNamespacesOperation(cqlSessionCache);
  }
}
