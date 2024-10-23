package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that alters the column definition in a table.")
@JsonTypeName("alterTable")
public record AlterTableCommand(AlterTableOperation operation) implements CollectionCommand {

  @Override
  public CommandName commandName() {
    return CommandName.ALTER_TABLE;
  }
}
