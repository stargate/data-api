package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that drops an index for a column.")
@JsonTypeName("dropIndex")
public record DropIndexCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name for the index to be dropped.")
        String name,
    @Nullable @Schema(description = "Dropping index command option.", type = SchemaType.OBJECT)
        Options options)
    implements NoOptionsCommand, CollectionCommand {

  public record Options(
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
