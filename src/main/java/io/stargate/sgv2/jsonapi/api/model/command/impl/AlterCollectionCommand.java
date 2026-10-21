package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Command that adds or removes an OpenSearch index on an existing collection. */
@Schema(description = "Command that adds or removes an OpenSearch index on an existing collection.")
@JsonTypeName(CommandName.Names.ALTER_COLLECTION)
public record AlterCollectionCommand(AlterCollectionOperation operation)
    implements CollectionCommand, NoOptionsCommand {
  @Override
  public CommandName commandName() {
    return CommandName.ALTER_COLLECTION;
  }
}
