package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorIndexDescDefaults;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonPropertyOrder({
  TableDescConstants.IndexDefinitionDesc.COLUMN,
  TableDescConstants.IndexDefinitionDesc.OPTIONS
})
public record VectorIndexDefinitionDesc(
    @NotNull
        @Schema(description = "Required name of the column to index.", required = true)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
        String column,
    //
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Indexing options.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.OPTIONS)
        VectorIndexDescOptions options)
    implements IndexDefinitionDesc<String, VectorIndexDefinitionDesc.VectorIndexDescOptions> {

  /** Options for the vector index */
  @JsonPropertyOrder({
    VectorConstants.VectorColumn.METRIC,
    VectorConstants.VectorColumn.SOURCE_MODEL
  })
  public record VectorIndexDescOptions(
      @Nullable
          @Pattern(
              regexp = SimilarityFunction.ApiConstants.ALL_REG_EXP,
              message = "Function name must be one of: " + SimilarityFunction.ApiConstants.ALL)
          @Schema(
              description =
                  "Optional Similarity function algorithm for sorting vectors, supported values are: "
                      + SimilarityFunction.ApiConstants.ALL,
              defaultValue = VectorIndexDescDefaults.DEFAULT_METRIC_NAME,
              type = SchemaType.STRING,
              implementation = String.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(VectorConstants.VectorColumn.METRIC)
          String metric,
      //
      @Nullable
          @Schema(
              description =
                  "Optional name of the model used to generate the embeddings, indexes can be optimized if the model name is known. Supported values are: "
                      + EmbeddingSourceModel.ApiConstants.ALL)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(VectorConstants.VectorColumn.SOURCE_MODEL)
          String sourceModel) {}
}
