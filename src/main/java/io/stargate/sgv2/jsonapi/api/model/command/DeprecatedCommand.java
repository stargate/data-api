package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.WarningException;

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
  CommandName useCommandName();

  /** A warning message will be added to commandResult when deprecated command is used. */
  default APIException getDeprecationWarning() {
    return WarningException.Code.DEPRECATED_COMMAND.get(
        "deprecatedCommand", this.commandName().getApiName(),
        "replacementCommand", useCommandName().getApiName());
  }
}
