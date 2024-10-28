package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSetType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Map;
import java.util.Objects;

/** Column type for {@link ApiSetType} */
public class SetColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc valueType;

  public SetColumnDesc(ColumnDesc valueType) {
    super(ApiTypeName.SET, ApiSupportDesc.fullSupport(""));
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
    io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.SetColumnDesc listType =
        (io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.SetColumnDesc) o;
    return Objects.equals(valueType, listType.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(valueType);
  }

  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    public SetColumnDesc create(String valueTypeName) {

      // step 1 - make sure value names are types we support
      // it would be better if we called all the way back to the top of the parsing the json
      // but we know they have to be primitive types
      var maybeValueDesc = PrimitiveColumnDesc.FROM_JSON_FACTORY.create(valueTypeName);

      // step 2 - create the list desc, and then to be sure get the ApiDataType to check it
      var setDesc = maybeValueDesc.map(SetColumnDesc::new).orElse(null);

      // hack - it's not a vector so ok to not have the vectorize validator
      if (setDesc == null || !ApiSetType.FROM_COLUMN_DESC_FACTORY.isSupported(setDesc, null)) {
        throw SchemaException.Code.UNSUPPORTED_SET_DEFINITION.get(
            Map.of(
                "supportedTypes",
                errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                "unsupportedValueType",
                typeNameOrMissing(valueTypeName)));
      }
      return setDesc;
    }
  }
}
