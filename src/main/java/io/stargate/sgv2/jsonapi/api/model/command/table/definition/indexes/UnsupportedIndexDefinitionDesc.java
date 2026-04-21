package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * An index definition that is not supported by the API.
 *
 * <p>It's a class not a record, and implements the IndexDefinitionDesc, so needs @JsonProperty
 * annotation because it is not using bean accessor methods.
 */
@JsonPropertyOrder({
  TableDescConstants.IndexDefinitionDesc.COLUMN,
  TableDescConstants.IndexDefinitionDesc.OPTIONS,
  TableDescConstants.IndexDefinitionDesc.API_SUPPORT
})
public class UnsupportedIndexDefinitionDesc implements IndexDefinitionDesc<String, Object> {

  private final ApiIndexSupportDesc apiIndexSupport;

  public UnsupportedIndexDefinitionDesc(ApiIndexSupportDesc apiIndexSupport) {
    this.apiIndexSupport = apiIndexSupport;
  }

  @NotNull
  @Schema(description = "Name of the column the index is on.")
  @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
  public String column() {
    return "UNKNOWN";
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @Schema(description = "Indexing options.", type = SchemaType.OBJECT)
  @JsonProperty(TableDescConstants.IndexDefinitionDesc.OPTIONS)
  public Object options() {
    // always null, then filtered out by Jackson
    return null;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @Schema(description = "Support for the index in the Data API.", type = SchemaType.OBJECT)
  @JsonProperty(TableDescConstants.IndexDefinitionDesc.API_SUPPORT)
  public ApiIndexSupportDesc apiSupport() {
    return apiIndexSupport;
  }
}
