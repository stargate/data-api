package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiListType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.Objects;

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

  /**
   * Factory to create a {@link ListColumnDesc} from a JSON node representing the type
   *
   * <p>...
   */
  public static class FromJsonFactory extends DescFromJsonFactory {

    FromJsonFactory() {}

    public ListColumnDesc create(JsonParser jsonParser, JsonNode valueTypeNode)
        throws JsonProcessingException {

      // Cascade deserialization to get the value type, validation is done when we
      // create the ApiDataType form the ColumnDesc
      var valueType =
          ColumnDescDeserializer.deserialize(
              valueTypeNode, jsonParser, TypeBindingPoint.COLLECTION_VALUE);

      return new ListColumnDesc(valueType);
    }
  }
}
