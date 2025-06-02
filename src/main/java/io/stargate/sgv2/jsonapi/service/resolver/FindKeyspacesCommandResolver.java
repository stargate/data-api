package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindKeyspacesCommand;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.FindKeyspacesOperation;
import jakarta.enterprise.context.ApplicationScoped;

/** Command resolver for {@link FindKeyspacesCommand}. */
@ApplicationScoped
public class FindKeyspacesCommandResolver implements CommandResolver<FindKeyspacesCommand> {

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
