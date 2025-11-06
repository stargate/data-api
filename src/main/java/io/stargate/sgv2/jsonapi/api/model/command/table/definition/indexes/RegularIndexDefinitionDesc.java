package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.RegularIndexColumnDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.serializer.RegularIndexColumnSerializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapComponentDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Definition of a regular index on a primitive or collection type (map,list, or set), for the
 * public API.
 *
 * <p>CreateIndexCommand on map column can have map component specified. Details see {@link
 * RegularIndexColumnDeserializer}
 */
@JsonPropertyOrder({
  TableDescConstants.IndexDefinitionDesc.COLUMN,
  TableDescConstants.IndexDefinitionDesc.OPTIONS
})
public record RegularIndexDefinitionDesc(
    @NotNull
        @Schema(
            description =
                "Required name of the column to index. To index the keys or values in a map column, use {\"my_map_colum\" : \"$keys\"} or {\"my_map_colum\" : \"$values\"}",
            required = true)
        @JsonDeserialize(using = RegularIndexColumnDeserializer.class)
        @JsonSerialize(using = RegularIndexColumnSerializer.class)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
        RegularIndexColumn column,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(
            description =
                "Options for the new index, not all options are valid for all data types. Check documentation for details.",
            type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.OPTIONS)
        RegularIndexDescOptions options)
    implements IndexDefinitionDesc<
        RegularIndexDefinitionDesc.RegularIndexColumn,
        RegularIndexDefinitionDesc.RegularIndexDescOptions> {

  public record RegularIndexColumn(
      @NotNull @Schema(description = "Required name of the column to index.", required = true)
          String columnName,
      @Nullable
          @Schema(description = "Optional component of a map column to index, $keys or $values")
          // Note, user can only specify $keys and $values, default will be resolved in entries
          // later.
          MapComponentDesc mapComponent) {}

  /**
   * Options for the index. Text and ascii primitive datatypes can have the analyzer options
   * specified. List/Set that has text and ascii primitive value can have the analyzer options
   * specified. Map index on keys, and keys are text and ascii primitive datatypes can have the
   * analyzer options specified. Map index on values, and values are text and ascii primitive
   * datatypes can have the analyzer options specified. Map index on entries is not supported for
   * analyzer options.
   */
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
          Boolean normalize) {

    public boolean isEmpty() {
      return ascii == null && caseSensitive == null && normalize == null;
    }

    @Override
    public String toString() {
      return "ascii=" + ascii + ", caseSensitive=" + caseSensitive + ", normalize=" + normalize;
    }
  }
}
