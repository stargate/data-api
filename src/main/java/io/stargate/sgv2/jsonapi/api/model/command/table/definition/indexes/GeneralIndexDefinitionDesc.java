package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonPropertyOrder({"column", "options"})
public record GeneralIndexDefinitionDesc(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the column for which index to be created.")
        String column,
    @Nullable
        @Schema(
            description = "mapIndex for indicating where to build index on mapColumn",
            type = SchemaType.STRING,
            implementation = String.class)
        @Pattern(
            regexp = "(keys|values|entries)",
            message = "support index functions are keys/values/entries")
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String indexFunction,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Different indexing options.", type = SchemaType.OBJECT)
        GeneralIndexDescOptions options)
    implements IndexDefinitionDesc<GeneralIndexDefinitionDesc.GeneralIndexDescOptions> {

  /**
   * Only text and ascii datatypes can be analyzed. <br>
   * Only text and ascii in the collection datatype can be analyzed. It works for values(list),
   * values(set), keys(map), values(map). Note, not for entries(map). <br>
   */
  @JsonPropertyOrder({"ascii", "caseSensitive", "mapIndex", "normalize"})
  public record GeneralIndexDescOptions(
      @Nullable
          @Schema(
              description =
                  "When set to true, index will converts alphabetic, numeric, and symbolic characters to the ascii equivalent, if one exists.",
              defaultValue = "false",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          Boolean ascii,
      @Nullable
          @Schema(
              description = "Ignore case in matching string values.",
              defaultValue = "true",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          Boolean caseSensitive,
      @Nullable
          @Schema(
              description = "When set to true, perform Unicode normalization on indexed strings.",
              defaultValue = "false",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          Boolean normalize) {}
}
