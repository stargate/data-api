package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that deletes a Keyspace.")
@JsonTypeName("dropKeyspace")
public record DropKeyspaceCommand(
    @NotNull
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Size(min = 1, max = 48)
        @Schema(description = "Name of the Keyspace")
        String name)
    implements GeneralCommand, NoOptionsCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DROP_KEYSPACE;
  }
}
