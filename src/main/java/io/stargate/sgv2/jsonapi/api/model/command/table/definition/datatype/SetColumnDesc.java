package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSetType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Column type for {@link ApiSetType} */
public class SetColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc valueType;

  public SetColumnDesc(ColumnDesc valueType) {
    this(valueType, ApiSupportDesc.withoutCqlDefinition(ApiSetType.API_SUPPORT));
  }

  public SetColumnDesc(ColumnDesc valueType, ApiSupportDesc apiSupportDesc) {
    super(ApiTypeName.SET, apiSupportDesc);
    this.valueType = valueType;
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
    var setDesc = (SetColumnDesc) o;
    return Objects.equals(valueType, setDesc.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueType);
  }

  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    /** Create a {@link SetColumnDesc} from a JSON node representing the value type. */
    public SetColumnDesc create(JsonParser jsonParser, JsonNode valueType)
        throws JsonMappingException {

      // step 1 - try to get the value type as a ColumnDesc
      Optional<ColumnDesc> valueTypeDesc =
          elementTypeForMapSetListColumn(jsonParser, valueType, true);

      // step 2 - create the list desc, and then to be sure get the ApiDataType to check it
      var setDesc = valueTypeDesc.map(SetColumnDesc::new).orElse(null);

      if (setDesc == null || !ApiSetType.FROM_COLUMN_DESC_FACTORY.isSupported(setDesc, null)) {
        throw SchemaException.Code.UNSUPPORTED_SET_DEFINITION.get(
            Map.of(
                "supportedTypes",
                errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                "unsupportedValueType",
                typeNameOrMissing(valueType)));
      }
      return setDesc;
    }
  }
}
