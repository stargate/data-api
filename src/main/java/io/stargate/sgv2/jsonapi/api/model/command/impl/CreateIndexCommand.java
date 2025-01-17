package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.IndexCreationCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.GeneralIndexDefinitionDesc;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates an index for a column in a table.")
@JsonTypeName(CommandName.Names.CREATE_INDEX)
public record CreateIndexCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the column to create the index on")
        String name,
    @Valid
        @NotNull
        @Schema(description = "Definition of the index to create.", type = SchemaType.OBJECT)
        GeneralIndexDefinitionDesc definition,
    @Nullable @Schema(description = "Type of the index to create.", type = SchemaType.STRING)
        String indexType,
    @Nullable @Schema(description = "Creating index command option.", type = SchemaType.OBJECT)
        CreateIndexCommandOptions options)
    implements CollectionCommand, IndexCreationCommand {

  // This is index command option irrespective of column definition.
  public record CreateIndexCommandOptions(
      @Schema(
              description = "Flag to ignore if index already exists",
              defaultValue = "false",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          Boolean ifNotExists) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_INDEX;
  }
}
