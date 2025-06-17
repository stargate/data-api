package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotEmpty;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command for dropping a table.
 *
 * @param name Name of the table
 */
@Schema(description = "Command that drops a type if one exists.")
@JsonTypeName(CommandName.Names.DROP_TYPE)
public record DropTypeCommand(
    @NotEmpty // prevent null or empty String from breaking CQL statement, validate early
        @Schema(description = "Required name of the type to remove")
        String name,
    @Nullable @Schema(description = "Dropping type command option.", type = SchemaType.OBJECT)
        Options options)
    implements NoOptionsCommand, KeyspaceCommand {

  public record Options(
      @Nullable
          @Schema(
              description = "Flag to ignore if type doesn't exists",
              defaultValue = "false",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          Boolean ifExists) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DROP_TABLE;
  }
}
