package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.impl.*;

/**
 * All deprecated commands will implement this interface. It has default deprecation message
 * implementation and useCommandName method tp return supported corresponding commandName
 */
public interface DeprecatedCommand extends Command {

  /**
   * DeprecatedCommand may have a corresponding new supported command This method will return the
   * commandName enum of supported command
   *
   * @return commandName enum
   */
  Command.CommandName useCommandName();

  /**
   * A warning message will be added to commandResult when deprecated command is used. Template:
   * This ${commandName} has been deprecated and will be removed in future releases, use
   * ${useCommandName} instead.
   *
   * @return String
   */
  default String getDeprecationMessage() {
    return String.format(
        "This %s has been deprecated and will be removed in future releases, use %s instead.",
        this.commandName().getApiName(), useCommandName().getApiName());
  }
}
