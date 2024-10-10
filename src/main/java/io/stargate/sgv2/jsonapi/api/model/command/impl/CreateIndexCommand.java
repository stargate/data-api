package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

// TODO, hide table feature detail before it goes public
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
            description = "Column definition for which index is created.",
            type = SchemaType.OBJECT)
        Definition definition)
    implements NoOptionsCommand, CollectionCommand {

  public record Definition(
      @NotNull
          @Size(min = 1, max = 48)
          @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
          @Schema(description = "Name of the column to create the index on")
          String column,
      @Nullable @Schema(description = "Options for creating index.", type = SchemaType.OBJECT)
          Options options) {

    public record Options(
        @Schema(
                description = "Flag to ignore if index already exists",
                defaultValue = "false",
                type = SchemaType.BOOLEAN,
                implementation = Boolean.class)
            Boolean ifNotExists,
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
            Boolean ascii,
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

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_INDEX;
  }
}
