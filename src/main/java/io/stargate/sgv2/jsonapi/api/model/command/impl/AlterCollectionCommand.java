package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that alters mutable settings of an existing collection. Currently supports enabling the 'lexical' feature.")
@JsonTypeName(CommandName.Names.ALTER_COLLECTION)
public record AlterCollectionCommand(AlterCollectionOperation operation)
    implements CollectionCommand, NoOptionsCommand {

  @Override
  public CommandName commandName() {
    return CommandName.ALTER_COLLECTION;
  }

  @Override
  public boolean isForceSchemaRefresh() {
    return true;
  }
}
