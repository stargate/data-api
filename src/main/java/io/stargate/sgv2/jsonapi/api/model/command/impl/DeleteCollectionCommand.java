package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command for deleting a collection.
 *
 * @param name Name of the collection
 */
@Schema(description = "Command that deletes a collection if one exists.")
@JsonTypeName("deleteCollection")
public record DeleteCollectionCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the collection")
        String name)
    implements CollectionOnlyCommand, NoOptionsCommand {

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
