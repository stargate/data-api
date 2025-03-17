package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available embedding providers.")
@JsonTypeName(CommandName.Names.FIND_EMBEDDING_PROVIDERS)
public record FindEmbeddingProvidersCommand() implements GeneralCommand, NoOptionsCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_EMBEDDING_PROVIDERS;
  }
}
