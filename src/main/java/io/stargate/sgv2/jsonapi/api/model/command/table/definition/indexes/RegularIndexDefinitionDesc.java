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
public record RegularIndexDefinitionDesc(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the column for which index to be created.")
        String column,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Different indexing options.", type = SchemaType.OBJECT)
        RegularIndexDescOptions options)
    implements IndexDefinitionDesc<RegularIndexDefinitionDesc.RegularIndexDescOptions> {

  /**
   * Only text and ascii datatypes can be analyzed. <br>
   * Only text and ascii in the collection datatype can be analyzed. It works for values(list),
   * values(set), keys(map), values(map). Note, not for entries(map). <br>
   */
  @JsonPropertyOrder({"ascii", "caseSensitive", "normalize"})
  public record RegularIndexDescOptions(
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
