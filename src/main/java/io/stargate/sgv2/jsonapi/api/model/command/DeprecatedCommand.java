package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;

/** Interface for all commands executed against a namespace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateNamespaceCommand.class),
  @JsonSubTypes.Type(value = DropNamespaceCommand.class),
  @JsonSubTypes.Type(value = FindNamespacesCommand.class),
})
public interface DeprecatedCommand extends Command {

  /**
   * DeprecatedCommand may have a corresponding new supported command This method will return the
   * commandName of supported command
   *
   * @return String
   */
  String useCommandName();

  /**
   * A warning message will be added to commandResult when deprecated command is used. Template:
   * This ${commandName} has been deprecated and will be removed in future releases, use
   * ${useCommandName} instead.
   *
   * @return String
   */
  String getDeprecationMessage();
}
