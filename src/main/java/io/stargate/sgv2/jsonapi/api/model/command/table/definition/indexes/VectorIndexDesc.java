package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

public record VectorIndexDesc(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the column for which index to be created.")
        String column,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Different indexing options.", type = SchemaType.OBJECT)
        CreateVectorIndexCommand.Options options) {

  // This is index definition options for vector column types.
  public record VectorIndexDescOptions(
      @Nullable
          @Pattern(
              regexp = "(dot_product|cosine|euclidean)",
              message = "function name can only be 'dot_product', 'cosine' or 'euclidean'")
          @Schema(
              description = "Similarity function algorithm that needs to be used for vector search",
              defaultValue = "cosine",
              type = SchemaType.STRING,
              implementation = String.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          SimilarityFunction metric,
      @Nullable
          @Size(min = 1, max = 48)
          @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
          @Schema(description = "Model name used to generate the embeddings.")
          @JsonInclude(JsonInclude.Include.NON_NULL)
          String sourceModel) {}
}
