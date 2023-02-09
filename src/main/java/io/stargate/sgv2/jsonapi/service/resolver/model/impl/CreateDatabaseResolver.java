package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateDatabaseCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;

public class CreateDatabaseResolver implements CommandResolver<CreateDatabaseCommand> {
    @Override public Class<CreateDatabaseCommand> getCommandClass() {
        return CreateDatabaseCommand.class;
    }

    @Override public Operation resolveCommand(CommandContext ctx, CreateDatabaseCommand command) {
        return null;
    }

}
