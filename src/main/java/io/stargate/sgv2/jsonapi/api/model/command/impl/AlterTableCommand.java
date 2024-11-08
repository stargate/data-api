package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that alters the column definition in a table.")
@JsonTypeName(Command.ALTER_TABLE_NAME)
public record AlterTableCommand(AlterTableOperation operation)
    implements CollectionCommand, NoOptionsCommand {
  @Override
  public CommandName commandName() {
    return CommandName.ALTER_TABLE;
  }
}
