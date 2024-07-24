package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.namespaces.FindNamespacesOperations;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindNamespacesCommand}. */
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
    return new FindNamespacesOperations(cqlSessionCache);
  }
}
