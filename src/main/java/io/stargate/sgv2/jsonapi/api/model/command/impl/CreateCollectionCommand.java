package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand;
import jakarta.validation.constraints.*;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a collection.")
@JsonTypeName("createCollection")
public record CreateCollectionCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the collection")
        String name,
    @Nullable
        @Schema(
            description = "Configuration for the collection",
            type = SchemaType.OBJECT,
            implementation = Options.class)
        Options options)
    implements NamespaceCommand {
  public record Options(

      // limit of returned documents
      @Schema(
              description = "Vector search index configuration for the collection",
              type = SchemaType.OBJECT,
              implementation = VectorSearchConfig.class)
          VectorSearchConfig vector) {

    public record VectorSearchConfig(
        @Positive(message = "size should be greater than `0`")
            @Schema(
                description = "Vector field embedding size",
                type = SchemaType.INTEGER,
                implementation = Integer.class)
            Integer size,
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
            String function) {}
  }
}
