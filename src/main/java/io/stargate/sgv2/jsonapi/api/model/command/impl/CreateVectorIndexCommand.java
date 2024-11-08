package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates an index for a column in a table.")
@JsonTypeName("createVectorIndex")
public record CreateVectorIndexCommand(
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
    // This is index definition options for vector column types.
    public record Options(
        @Nullable
            @Pattern(
                regexp = "(dot_product|cosine|euclidean)",
                message = "function name can only be 'dot_product', 'cosine' or 'euclidean'")
            @Schema(
                description =
                    "Similarity function algorithm that needs to be used for vector search",
                defaultValue = "cosine",
                type = SchemaType.STRING,
                implementation = String.class)
            SimilarityFunction metric,
        @Nullable
            @Size(min = 1, max = 48)
            @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
            @Schema(description = "Model name used to generate the embeddings.")
            String sourceModel) {}
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
