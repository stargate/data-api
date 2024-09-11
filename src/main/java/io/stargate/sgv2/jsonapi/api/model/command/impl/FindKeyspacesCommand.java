package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available keyspaces")
@JsonTypeName("findKeyspaces")
public record FindKeyspacesCommand() implements GeneralCommand, NoOptionsCommand {

  /**
   * Override Command interface, this method return the class name of implementation class
   *
   * @return String
   */
  @Override
  public String commandName() {
    return this.getClass().getSimpleName();
  }
}
