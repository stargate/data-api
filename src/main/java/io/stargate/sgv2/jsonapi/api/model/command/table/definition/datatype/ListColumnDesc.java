package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiListType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Objects;

/** Column type for {@link ApiListType} */
public class ListColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private final ColumnDesc valueType;

  /** Creates a column desc from user input, without ApiSupport Description. */
  public ListColumnDesc(SchemaDescSource schemaDescSource, ColumnDesc valueType) {
    this(schemaDescSource, valueType, null);
  }

  public ListColumnDesc(
      SchemaDescSource schemaDescSource, ColumnDesc valueType, ApiSupportDesc apiSupportDesc) {
    super(schemaDescSource, ApiTypeName.LIST, apiSupportDesc);
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
  public static class FromJsonFactory extends ColumnDescFromJsonFactory<ListColumnDesc> {

    FromJsonFactory() {}

    @Override
    public ListColumnDesc create(
        String columnName,
        SchemaDescSource schemaDescSource,
        JsonParser jsonParser,
        JsonNode columnDescNode)
        throws JsonProcessingException {

      // Cascade deserialization to get the value type, validation is done when we
      // create the ApiDataType form the ColumnDesc
      var valueTypeNode = columnDescNode.path(TableDescConstants.ColumnDesc.VALUE_TYPE);
      var valueType =
          valueTypeNode.isMissingNode()
              ? null
              : ColumnDescDeserializer.deserialize(
                  columnName, valueTypeNode, jsonParser, schemaDescSource);

      return new ListColumnDesc(schemaDescSource, valueType);
    }
  }
}
