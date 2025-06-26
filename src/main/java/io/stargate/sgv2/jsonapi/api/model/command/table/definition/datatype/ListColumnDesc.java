package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiListType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Column type for {@link ApiListType} */
public class ListColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc valueType;

  public ListColumnDesc(ColumnDesc valueType) {
    this(valueType, ApiSupportDesc.withoutCqlDefinition(ApiListType.API_SUPPORT));
  }

  public ListColumnDesc(ColumnDesc valueType, ApiSupportDesc apiSupportDesc) {
    super(ApiTypeName.LIST, apiSupportDesc);
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
    var listDesc = (ListColumnDesc) o;
    return Objects.equals(valueType, listDesc.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueType);
  }

  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    /** Create a {@link ListColumnDesc} from a JSON node representing the value type. */
    public ListColumnDesc create(JsonParser jsonParser, JsonNode valueType)
        throws JsonMappingException {

      // step 1 - try to get the value type as a ColumnDesc
      Optional<ColumnDesc> valueTypeDesc =
          elementTypeForMapSetListColumn(jsonParser, valueType, true);
      // step 2 - create the list desc, and then to be sure get the ApiDataType to check it
      var listDesc = valueTypeDesc.map(ListColumnDesc::new).orElse(null);

      if (listDesc == null || !ApiListType.FROM_COLUMN_DESC_FACTORY.isSupported(listDesc, null)) {
        throw SchemaException.Code.UNSUPPORTED_LIST_DEFINITION.get(
            Map.of(
                "supportedTypes",
                errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                "unsupportedValueType",
                typeNameOrMissing(valueType)));
      }
      return listDesc;
    }
  }
}
