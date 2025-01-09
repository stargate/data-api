package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command for dropping a table.
 *
 * @param name Name of the table
 */
@Schema(description = "Command that drops a table if one exists.")
@JsonTypeName(CommandName.Names.DROP_TABLE)
public record DropTableCommand(
    @NotNull @Schema(description = "Name of the table") @NotEmpty // prevent error from empty String
        String name,
    @Nullable @Schema(description = "Dropping table command option.", type = SchemaType.OBJECT)
        Options options)
    implements NoOptionsCommand, KeyspaceCommand {

  public record Options(
      @Nullable
          @Schema(
              description = "Flag to ignore if table doesn't exists",
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
