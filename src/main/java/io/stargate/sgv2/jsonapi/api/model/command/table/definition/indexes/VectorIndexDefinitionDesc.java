package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.VectorIndexingDescDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.config.constants.VectorIndexDescDefaults;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Map;
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
    VectorConstants.VectorColumn.SOURCE_MODEL,
    VectorConstants.VectorColumn.VECTOR_INDEXING
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
          String sourceModel,
      //
      @Nullable
          @Schema(
              description =
                  "Optional vector (SAI) indexing configuration. Either a profile name (string) "
                      + "the API expands into SAI options, e.g. \"small-high-recall\"; or an object "
                      + "of Cassandra SAI tuning options (snake_case), restricted to: "
                      + "maximum_node_connections, construction_beam_width, neighborhood_overflow, "
                      + "alpha, enable_hierarchy, e.g. {\"maximum_node_connections\": 32, "
                      + "\"alpha\": 1.2}. A profile and explicit options are mutually exclusive. "
                      + "Set \"metric\" / \"sourceModel\" via their dedicated fields, not here.")
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(VectorConstants.VectorColumn.VECTOR_INDEXING)
          VectorIndexingDesc vectorIndexing) {}

  /**
   * Overloaded {@code vectorIndexing} value: exactly one of a named {@code profile} (JSON string)
   * or raw SAI tuning {@code options} (JSON object) is set. {@link VectorIndexingDescDeserializer}
   * discriminates by JSON type; {@link #jsonValue()} serializes back to the bare string or object.
   */
  @JsonDeserialize(using = VectorIndexingDescDeserializer.class)
  public record VectorIndexingDesc(
      @Nullable String profile, @Nullable Map<String, Object> options) {

    /** A {@code vectorIndexing} that selects a named profile. */
    public static VectorIndexingDesc ofProfile(String profile) {
      return new VectorIndexingDesc(profile, null);
    }

    /** A {@code vectorIndexing} that sets raw SAI options directly. */
    public static VectorIndexingDesc ofOptions(Map<String, Object> options) {
      return new VectorIndexingDesc(null, options);
    }

    /** Serializes to the bare profile string or the bare options map. */
    @JsonValue
    Object jsonValue() {
      return profile != null ? profile : options;
    }
  }
}
