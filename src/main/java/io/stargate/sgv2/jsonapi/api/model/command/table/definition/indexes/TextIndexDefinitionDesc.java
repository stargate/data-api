package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonPropertyOrder({
  TableDescConstants.IndexDefinitionDesc.COLUMN,
  TableDescConstants.IndexDefinitionDesc.OPTIONS
})
public record TextIndexDefinitionDesc(
    @NotNull
        @Schema(description = "Required name of the column to index.", required = true)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
        String column,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Indexing options.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.OPTIONS)
        TextIndexDescOptions options)
    implements IndexDefinitionDesc<String, TextIndexDefinitionDesc.TextIndexDescOptions> {

  /** Options for the vector index */
  @JsonPropertyOrder({TableDescConstants.TextIndexDefinitionDescOptions.ANALYZER})
  public record TextIndexDescOptions(
      @Nullable
          @Schema(
              description =
                  "Optional definition of the analyzer to use for the text index. If not specified, the default analyzer (\"standard\") will be used.")
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(TableDescConstants.TextIndexDefinitionDescOptions.ANALYZER)
          JsonNode analyzer) {}
}
