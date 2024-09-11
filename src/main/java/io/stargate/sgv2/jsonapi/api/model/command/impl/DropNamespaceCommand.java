package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command {@link DropNamespaceCommand} is deprecated, please switch to {@link DropKeyspaceCommand}
 * Support it for backward-compatibility
 */
@Schema(
    description =
        "Command that deletes a namespace. This command has been deprecated and will be removed in future releases, use DropNamespaceCommand instead.",
    deprecated = true)
@JsonTypeName("dropNamespace")
public record DropNamespaceCommand(
    @NotNull
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Size(min = 1, max = 48)
        @Schema(
            description =
                "Name of the namespace. This command has been deprecated and will be removed in future releases, use DropNamespaceCommand instead.",
            deprecated = true)
        String name)
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
    return DropKeyspaceCommand.class.getSimpleName();
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
