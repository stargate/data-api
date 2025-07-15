package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import java.util.Objects;

/**
 * Description of a column that uses a user-defined type (UDT) as its data type. This is only used
 * when the column is a reference to a UDT, not when it is a full definition of a UDT if also
 * needed. TODO: XXXX where is the full UDT def ?
 *
 * <p>Creates instances using the {@link #FROM_JSON_FACTORY} when creating from JSON
 *
 * <p>NOTE: a column using a UDT only specifies the name of the UDT, not the fields of the UDT. The
 * user description for the full UDT is in {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTypeCommand} and {@link TypeDefinitionDesc}
 *
 * <p>See {@link io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer}
 * for examples of how this is deserialized from JSON.
 */
public class UdtRefColumnDesc extends ComplexColumnDesc {

  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  protected final CqlIdentifier udtName;

  public UdtRefColumnDesc(SchemaDescSource schemaDescSource, CqlIdentifier udtName) {
    this(schemaDescSource, udtName, null);
  }

  public UdtRefColumnDesc(
      SchemaDescSource schemaDescSource, CqlIdentifier udtName, ApiSupportDesc apiSupportDesc) {
    super(schemaDescSource, ApiTypeName.UDT, apiSupportDesc);

    this.udtName = Objects.requireNonNull(udtName, "udtName must not be null");
  }

  public CqlIdentifier udtName() {
    return udtName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var udtColumnDesc = (UdtRefColumnDesc) o;
    return Objects.equals(udtName, udtColumnDesc.udtName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(udtName);
  }

  /**
   * Factory to create a {@link UdtRefColumnDesc} from JSON representing a reference to the udt in a
   * column definition.
   *
   * <p>...
   */
  public static class FromJsonFactory extends ColumnDescFromJsonFactory<UdtRefColumnDesc> {
    FromJsonFactory() {}

    /**
     * Creates a {@link UdtRefColumnDesc} from JSON
     *
     * @param columnDescNode the whole column desc object <code>
     *     {"type": "userDefined", "udtName": "myUdt"}</code>
     * @return
     */
    public UdtRefColumnDesc create(
        SchemaDescSource schemaDescSource, JsonParser jsonParser, JsonNode columnDescNode) {

      Objects.requireNonNull(columnDescNode, "columnDescNode must not be null");

      // Validation is done when we create the ApiUdtType from the ColumnDesc,
      // the Missing node will return an empty text
      var udtName = columnDescNode.path(TableDescConstants.ColumnDesc.UDT_NAME).asText();

      return new UdtRefColumnDesc(schemaDescSource, cqlIdentifierFromUserInput(udtName));
    }
  }
}
