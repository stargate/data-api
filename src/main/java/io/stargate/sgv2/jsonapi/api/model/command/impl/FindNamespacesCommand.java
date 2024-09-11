package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command {@link FindNamespacesCommand} is deprecated, please switch to {@link
 * FindKeyspacesCommand} Support it for backward-compatibility
 */
@Schema(
    description =
        "Command that lists all available namespaces. This command has been deprecated and will be removed in future releases, use FindKeyspacesCommand instead.",
    deprecated = true)
@JsonTypeName("findNamespaces")
public record FindNamespacesCommand()
    implements GeneralCommand, NoOptionsCommand, DeprecatedCommand {
  /**
   * Override Command interface, this method return the class name of implementation class
   *
   * @return String
   */
  @Override
  public String commandName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Override DeprecatedCommand interface, this method return the class name of corresponding
   * supported command
   *
   * @return String
   */
  @Override
  public String useCommandName() {
    return FindKeyspacesCommand.class.getSimpleName();
  }

  /**
   * Override DeprecatedCommand interface, get the deprecation message for this implementation
   * command
   *
   * @return String
   */
  @Override
  public String getDeprecationMessage() {
    return String.format(
        "This %s has been deprecated and will be removed in future releases, use %s instead.",
        commandName(), useCommandName());
  }
}
