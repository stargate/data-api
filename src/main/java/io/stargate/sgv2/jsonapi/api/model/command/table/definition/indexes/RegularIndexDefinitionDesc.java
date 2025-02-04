package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonPropertyOrder({
  TableDescConstants.IndexDefinitionDesc.COLUMN,
  TableDescConstants.IndexDefinitionDesc.OPTIONS
})
public record RegularIndexDefinitionDesc(
    @NotNull
        @Schema(description = "Name of the column to index.", required = true)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
        String column,
    //
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Indexing options.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.OPTIONS)
        RegularIndexDescOptions options)
    implements IndexDefinitionDesc<RegularIndexDefinitionDesc.RegularIndexDescOptions> {

  /** Options for the index, used only with text, including text in a collection */
  @JsonPropertyOrder({
    TableDescConstants.RegularIndexDefinitionDescOptions.ASCII,
    TableDescConstants.RegularIndexDefinitionDescOptions.CASE_SENSITIVE,
    TableDescConstants.RegularIndexDefinitionDescOptions.NORMALIZE
  })
  public record RegularIndexDescOptions(
      @Nullable
          @Schema(
              description =
                  "When set to true, index will converts alphabetic, numeric, and symbolic characters to the ascii equivalent, if one exists.",
              defaultValue = TableDescDefaults.RegularIndexDescDefaults.Constants.ASCII,
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(TableDescConstants.RegularIndexDefinitionDescOptions.ASCII)
          Boolean ascii,
      //
      @Nullable
          @Schema(
              description = "When set to true, Ignore case when matching string values.",
              defaultValue = TableDescDefaults.RegularIndexDescDefaults.Constants.CASE_SENSITIVE,
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(TableDescConstants.RegularIndexDefinitionDescOptions.CASE_SENSITIVE)
          Boolean caseSensitive,
      //
      @Nullable
          @Schema(
              description = "When set to true, perform Unicode normalization on indexed strings.",
              defaultValue = TableDescDefaults.RegularIndexDescDefaults.Constants.NORMALIZE,
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          @JsonProperty(TableDescConstants.RegularIndexDefinitionDescOptions.NORMALIZE)
          Boolean normalize) {}
}
