package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindKeyspacesCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.FindKeyspacesOperation;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for {@link FindKeyspacesCommand}. */
@ApplicationScoped
public class FindKeyspacesCommandResolver extends FindNamespacesKeyspacesCommandResolver<FindKeyspacesCommand> {

  public FindKeyspacesCommandResolver() {}

  /** {@inheritDoc} */
  @Override
  public Class<FindKeyspacesCommand> getCommandClass() {
    return FindKeyspacesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, FindKeyspacesCommand command) {
    return new FindKeyspacesOperation(true);
  }
}
