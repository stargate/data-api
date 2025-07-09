package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiMapType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Objects;

/** Column type for {@link ApiMapType} */
public class MapColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc keyType;
  private final ColumnDesc valueType;

  /** Creates a column desc from user input, without ApiSupport Description. */
  public MapColumnDesc(
      SchemaDescSource schemaDescSource, ColumnDesc keyType, ColumnDesc valueType) {
    this(schemaDescSource, keyType, valueType, null);
  }

  public MapColumnDesc(
      SchemaDescSource schemaDescSource,
      ColumnDesc keyType,
      ColumnDesc valueType,
      ApiSupportDesc apiSupportDesc) {
    super(schemaDescSource, ApiTypeName.MAP, apiSupportDesc);

    this.keyType = keyType;
    this.valueType = valueType;
  }

  public ColumnDesc keyType() {
    return keyType;
  }

  public ColumnDesc valueType() {
    return valueType;
  }

  // Needed for testing
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var mapDesc = (MapColumnDesc) o;
    return Objects.equals(keyType, mapDesc.keyType) && Objects.equals(valueType, mapDesc.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType);
  }

  /**
   * Factory to create a {@link MapColumnDesc} from JSON nodes representing type
   *
   * <p>...
   */
  public static class FromJsonFactory extends ColumnDescFromJsonFactory<MapColumnDesc> {
    FromJsonFactory() {}

    /** Create a {@link MapColumnDesc} from key and value type jsonNodes. */
    public MapColumnDesc create(
        SchemaDescSource schemaDescSource, JsonParser jsonParser, JsonNode columnDescNode)
        throws JsonProcessingException {

      // Cascade deserialization to get the value type, validation is done when we
      // create the ApiDataType form the ColumnDesc
      var valueTypeNode = columnDescNode.path(TableDescConstants.ColumnDesc.VALUE_TYPE);
      var valueType =
          valueTypeNode.isMissingNode()
              ? null
              : ColumnDescDeserializer.deserialize(valueTypeNode, jsonParser, schemaDescSource);

      var keyTypeNode = columnDescNode.path(TableDescConstants.ColumnDesc.KEY_TYPE);
      var keyType =
          keyTypeNode.isMissingNode()
              ? null
              : ColumnDescDeserializer.deserialize(keyTypeNode, jsonParser, schemaDescSource);

      return new MapColumnDesc(schemaDescSource, keyType, valueType);
    }
  }
}
