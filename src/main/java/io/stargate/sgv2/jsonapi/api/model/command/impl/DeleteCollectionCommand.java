package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command for deleting a collection.
 *
 * @param name Name of the collection
 */
@Schema(description = "Command that deletes a collection if one exists.")
@JsonTypeName(CommandName.Names.DELETE_COLLECTION)
public record DeleteCollectionCommand(
    @NotNull @Schema(description = "Name of the collection") String name)
    implements CollectionOnlyCommand, NoOptionsCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DELETE_COLLECTION;
  }
}
