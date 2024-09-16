package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that deletes a namespace. This dropNamespace has been deprecated and will be removed in future releases, use dropKeyspace instead.",
    deprecated = true)
@JsonTypeName("dropNamespace")
public record DropNamespaceCommand(
    @NotNull
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Size(min = 1, max = 48)
        @Schema(
            description =
                "Name of the namespace. This dropNamespace has been deprecated and will be removed in future releases, use dropKeyspace instead.",
            deprecated = true)
        String name)
    implements GeneralCommand, NoOptionsCommand, DeprecatedCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DROP_NAMESPACE;
  }

  /** {@inheritDoc} */
  @Override
  public CommandName useCommandName() {
    return CommandName.DROP_KEYSPACE;
  }
}
