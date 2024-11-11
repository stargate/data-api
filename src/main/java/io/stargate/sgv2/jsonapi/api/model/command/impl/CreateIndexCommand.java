package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates an index for a column in a table.")
@JsonTypeName("createIndex")
public record CreateIndexCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the column to create the index on")
        String name,
    @NotNull
        @Schema(
            description = "Definition for created index for a column.",
            type = SchemaType.OBJECT)
        Definition definition,
    @Nullable @Schema(description = "Creating index command option.", type = SchemaType.OBJECT)
        Options options)
    implements CollectionCommand {
  public record Definition(
      @NotNull
          @Size(min = 1, max = 48)
          @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
          @Schema(description = "Name of the column for which index to be created.")
          String column,
      @Nullable @Schema(description = "Different indexing options.", type = SchemaType.OBJECT)
          Options options) {
    // This is index definition options for text column types.
    public record Options(
        @Nullable
            @Schema(
                description = "Ignore case in matching string values.",
                defaultValue = "true",
                type = SchemaType.BOOLEAN,
                implementation = Boolean.class)
            Boolean caseSensitive,
        @Nullable
            @Schema(
                description = "When set to true, perform Unicode normalization on indexed strings.",
                defaultValue = "false",
                type = SchemaType.BOOLEAN,
                implementation = Boolean.class)
            Boolean normalize,
        @Nullable
            @Schema(
                description =
                    "When set to true, index will converts alphabetic, numeric, and symbolic characters to the ascii equivalent, if one exists.",
                defaultValue = "false",
                type = SchemaType.BOOLEAN,
                implementation = Boolean.class)
            Boolean ascii) {}
  }

  // This is index command option irrespective of column definition.
  public record Options(
      @Nullable
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
