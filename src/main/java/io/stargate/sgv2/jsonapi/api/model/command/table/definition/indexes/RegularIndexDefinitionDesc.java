package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.config.constants.TableDescDefaults;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexFunction;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiRegularIndex;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonPropertyOrder({
  TableDescConstants.IndexDefinitionDesc.COLUMN,
  TableDescConstants.IndexDefinitionDesc.OPTIONS
})
public record RegularIndexDefinitionDesc(

    /*
     * API regular column can be primitive or map/set/list collection type.
     * CreateIndexCommand on collection column will have index function.
     * User can specify or use the default index function.
     * Details see {@link RegularIndexColumnDeserializer}
     */
    @NotNull
        @Schema(
            description =
                "Name of the column to index. (Optional: can also specify the index function for map column. E.G. {\"column\": {\"mapColumn\" : \"$keys\"}}",
            required = true)
        @JsonDeserialize(using = RegularIndexColumnDeserializer.class)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
        RegularIndexColumn column,
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(description = "Indexing options.", type = SchemaType.OBJECT)
        @JsonProperty(TableDescConstants.IndexDefinitionDesc.OPTIONS)
        RegularIndexDescOptions options)
    implements IndexDefinitionDesc<
        RegularIndexDefinitionDesc.RegularIndexColumn,
        RegularIndexDefinitionDesc.RegularIndexDescOptions> {

  public record RegularIndexColumn(
      @NotNull
          @Schema(description = "Name of the column to index.", required = true)
          @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
          String columnName,
      @Nullable
          @Schema(description = "Index function for map/set/list column.")
          @JsonProperty(TableDescConstants.IndexDefinitionDesc.COLUMN)
          ApiIndexFunction indexFunction) {}

  /**
   * Deserializes the RegularIndexColumn from json node of column.
   *
   * <p>Primitive column: {"column": "age"}, indexFunction is null
   *
   * <p>- List column: {"column": "listColumn"}
   *
   * <p>- Set column: {"column": "setColumn"}
   *
   * <p>- Map column:
   *
   * <pre>
   *   - Default to index on map entries: {"column": "mapColumn"}
   *   - Index on map keys: {"column": {"mapColumn" : "$keys"}}
   *   - Index on map values: {"column": {"mapColumn" : "$values"}}
   *   - Index on map entries: {"column": {"mapColumn" : "$entries"}}
   *   </pre>
   *
   * NOTE, this is just index function from user input, validation and default values are in {@link
   * ApiRegularIndex} since we need the ColumnMetaData
   */
  private static class RegularIndexColumnDeserializer extends StdDeserializer<RegularIndexColumn> {
    protected RegularIndexColumnDeserializer() {
      super(RegularIndexColumn.class);
    }

    @Override
    public RegularIndexColumn deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JacksonException {
      JsonNode columnNode = deserializationContext.readTree(jsonParser);

      if (columnNode.isTextual()) {
        return new RegularIndexColumn(columnNode.textValue(), null);
      } else if (columnNode.isObject() && columnNode.size() == 1) {
        Map.Entry<String, JsonNode> entry = columnNode.fields().next();

        if (!entry.getValue().isTextual()) {
          // E.G. {"column": {"mapColumn" : 123}}
          throw SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN.get();
        }
        return new RegularIndexColumn(
            entry.getKey(), ApiIndexFunction.fromDollarCommand(entry.getValue().textValue()));
      } else {
        throw SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN.get();
      }
    }
  }

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
