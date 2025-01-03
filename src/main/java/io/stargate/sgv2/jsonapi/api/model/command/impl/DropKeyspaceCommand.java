package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that deletes a Keyspace.")
@JsonTypeName(CommandName.Names.DROP_KEYSPACE)
public record DropKeyspaceCommand(
    @NotNull @Schema(description = "Name of the Keyspace") String name)
    implements GeneralCommand, NoOptionsCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DROP_KEYSPACE;
  }
}
