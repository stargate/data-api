package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiListType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Map;
import java.util.Objects;

/** Column type for {@link ApiListType} */
public class ListColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc valueType;

  public ListColumnDesc(ColumnDesc valueType) {
    super(ApiTypeName.LIST, ApiSupportDesc.fullSupport(""));
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
    io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ListColumnDesc listType =
        (io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ListColumnDesc) o;
    return Objects.equals(valueType, listType.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueType);
  }

  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    public ListColumnDesc create(String valueTypeName) {

      // step 1 - make sure value names are types we support
      // it would be better if we called all the way back to the top of the parsing the json
      // but we know they have to be primitive types
      var maybeValueDesc = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(valueTypeName);

      // step 2 - create the list desc, and then to be sure get the ApiDataType to check it
      var listDesc = maybeValueDesc.map(ListColumnDesc::new).orElse(null);

      // hack - it's not a vector so ok to not have the vectorize validator
      if (listDesc == null || !ApiListType.FROM_COLUMN_DESC_FACTORY.isSupported(listDesc, null)) {
        throw SchemaException.Code.UNSUPPORTED_LIST_DEFINITION.get(
            Map.of(
                "supportedTypes",
                errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                "unsupportedValueType",
                typeNameOrMissing(valueTypeName)));
      }
      return listDesc;
    }
  }
}
