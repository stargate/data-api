package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

public class DeprecatedCommandUtil {

  /**
   * This is a helper function to resolve deprecated command to a corresponding supported command
   * name We do not want to track deprecated command in metrics and logs, that is why we need to do
   * this convert.
   *
   * @param command command that may be deprecated
   * @return the command class simpleName
   */
  public static String maybeResolveDeprecatedCommand(Command command) {
    if (command instanceof FindNamespacesCommand) {
      return FindKeyspacesCommand.class.getSimpleName();
    }
    if (command instanceof DropNamespaceCommand) {
      return DropKeyspaceCommand.class.getSimpleName();
    }
    if (command instanceof CreateNamespaceCommand) {
      return CreateKeyspaceCommand.class.getSimpleName();
    }
    return command.getClass().getSimpleName();
  }

  /**
   * This is a helper function to resolve deprecated command to a corresponding supported command
   * name We do not want to track deprecated command in metrics and logs, that is why we need to do
   * this convert.
   *
   * @param command command string that may be deprecated
   * @return the command class simpleName
   */
  public static String maybeResolveDeprecatedCommandName(String command) {
    if (command.equals(FindNamespacesCommand.class.getSimpleName())) {
      return FindKeyspacesCommand.class.getSimpleName();
    }
    if (command.equals(DropNamespaceCommand.class.getSimpleName())) {
      return DropKeyspaceCommand.class.getSimpleName();
    }
    if (command.equals(CreateNamespaceCommand.class.getSimpleName())) {
      return CreateKeyspaceCommand.class.getSimpleName();
    }
    return command;
  }

  /**
   * Resolve a possible deprecated original command name from commandContext to supported command
   * Name, and generate warning message.
   *
   * <p>We don't want command as suffix, as it is not the actually command json that user needs e.g.
   * "deprecated command \"FindNamespacesCommand\", please switch to
   * "deprecated command \"FindNamespaces\", please switch to \"FindKeyspaces\"."
   *
   * @param commandContext commandContext that has original command name
   * @return a message states using of deprecated command
   */
  public static <U extends SchemaObject> String getDeprecatedCommandMsg(
      CommandContext<U> commandContext) {
    String supportedCommandName = maybeResolveDeprecatedCommandName(commandContext.commandName());
    String warningMessage = "deprecated command \"%s\", please switch to \"%s\".";
    if (!commandContext.commandName().equals(supportedCommandName)) {
      // using a deprecated command
      String deprecatedCommandJson =
          commandContext
              .commandName()
              .substring(0, commandContext.commandName().length() - "Command".length());
      String supportedCommandJson =
          supportedCommandName.substring(0, supportedCommandName.length() - "Command".length());
      return String.format(warningMessage, deprecatedCommandJson, supportedCommandJson);
    }
    return null;
  }
}
