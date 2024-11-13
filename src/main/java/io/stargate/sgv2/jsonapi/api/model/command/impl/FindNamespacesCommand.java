package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that lists all available namespaces. This findNamespaces has been deprecated and will be removed in future releases, use findKeyspaces instead.",
    deprecated = true)
@JsonTypeName("findNamespaces")
public record FindNamespacesCommand()
    implements GeneralCommand, NoOptionsCommand, DeprecatedCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_NAMESPACES;
  }

  /** {@inheritDoc} */
  @Override
  public CommandName useCommandName() {
    return CommandName.FIND_KEYSPACES;
  }
}
