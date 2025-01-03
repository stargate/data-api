package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.config.constants.VectorIndexDescDefaults;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonPropertyOrder({"column", "options"})
public record VectorIndexDefinitionDesc(
    @NotNull
        @Schema(description = "Name of the column for which index to be created.")
        String column,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Different indexing options.", type = SchemaType.OBJECT)
        VectorIndexDescOptions options)
    implements IndexDefinitionDesc<VectorIndexDefinitionDesc.VectorIndexDescOptions> {

  // This is index definition options for vector column types
  @JsonPropertyOrder({"metric", "sourceModel"})
  public record VectorIndexDescOptions(
      @Nullable
          @Pattern(
              regexp = "(dot_product|cosine|euclidean)",
              message = "function name can only be 'dot_product', 'cosine' or 'euclidean'")
          @Schema(
              description = "Similarity function algorithm that needs to be used for vector search",
              defaultValue = VectorIndexDescDefaults.DEFAULT_METRIC_NAME,
              type = SchemaType.STRING,
              implementation = String.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          String metric,
      @Nullable
          @Schema(description = "Model name used to generate the embeddings.")
          @JsonInclude(JsonInclude.Include.NON_NULL)
          String sourceModel) {}
}
