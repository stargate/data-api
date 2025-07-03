package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiMapType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.Objects;

/** Column type for {@link ApiMapType} */
public class MapColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc keyType;
  private final ColumnDesc valueType;

  public MapColumnDesc(ColumnDesc keyType, ColumnDesc valueType) {
    this(keyType, valueType, ApiSupportDesc.withoutCqlDefinition(ApiMapType.API_SUPPORT));
  }

  public MapColumnDesc(ColumnDesc keyType, ColumnDesc valueType, ApiSupportDesc apiSupportDesc) {
    super(ApiTypeName.MAP, apiSupportDesc);

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
  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    /** Create a {@link MapColumnDesc} from key and value type jsonNodes. */
    public MapColumnDesc create(JsonParser jsonParser, JsonNode keyTypeNode, JsonNode valueTypeNode)
        throws JsonProcessingException {

      // Cascade deserialization to get the key and value types, validation is done when we
      // create the ApiDataType form the ColumnDesc
      var valueType =
          ColumnDescDeserializer.deserialize(
              valueTypeNode, jsonParser, TypeBindingPoint.COLLECTION_VALUE);
      var keyType =
          ColumnDescDeserializer.deserialize(keyTypeNode, jsonParser, TypeBindingPoint.MAP_KEY);

      return new MapColumnDesc(keyType, valueType);
    }
  }
}
