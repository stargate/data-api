package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command that drops an index of a column using index name.
 *
 * @param name
 * @param options
 */
@Schema(description = "Command that drops an index for a column.")
@JsonTypeName(CommandName.Names.DROP_INDEX)
public record DropIndexCommand(
    @NotNull @Schema(description = "Name for the index to be dropped.") String name,
    @Nullable @Schema(description = "Dropping index command option.", type = SchemaType.OBJECT)
        Options options)
    implements NoOptionsCommand, KeyspaceCommand {

  public record Options(
      @Nullable
          @Schema(
              description = "Flag to ignore if index doesn't exists",
              defaultValue = "false",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          Boolean ifExists) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DROP_INDEX;
  }
}
