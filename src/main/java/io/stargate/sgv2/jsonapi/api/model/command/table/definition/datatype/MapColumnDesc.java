package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiMapType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Map;
import java.util.Objects;

/** Column type for {@link ApiMapType} */
public class MapColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc keyType;
  private final ColumnDesc valueType;

  public MapColumnDesc(ColumnDesc keyType, ColumnDesc valueType) {
    super(ApiTypeName.MAP, ApiSupportDesc.fullSupport(""));

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

  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    public MapColumnDesc create(String keyTypeName, String valueTypeName) {

      // step 1 - make sure the key and value names are types we support
      // it would be better if we called all the way back to the top of the parsing the json
      // but we know they have to be primitive types
      var maybeKeyDesc = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(keyTypeName);
      var maybeValueDesc = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(valueTypeName);

      // step 2 - create the map desc, and then to be sure get the ApiDataType to check it
      // cannot make the map desc if we cannot map the primary types
      var mapDesc =
          (maybeKeyDesc.isEmpty() || maybeValueDesc.isEmpty())
              ? null
              : new MapColumnDesc(maybeKeyDesc.get(), maybeValueDesc.get());

      // hack - it's not a vector so ok to not have the vectorize validator
      if (mapDesc == null || !ApiMapType.FROM_COLUMN_DESC_FACTORY.isSupported(mapDesc, null)) {
        throw SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.get(
            Map.of(
                "supportedTypes", errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                "unsupportedKeyType", typeNameOrMissing(keyTypeName),
                "unsupportedValueType", typeNameOrMissing(valueTypeName)));
      }
      return mapDesc;
    }
  }
}
