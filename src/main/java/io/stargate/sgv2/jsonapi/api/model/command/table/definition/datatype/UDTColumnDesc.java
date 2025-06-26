package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a User Defined Type (UDT) column in a table definition.
 *
 * <p>This class is used to describe a UDT column, including its name, fields, and whether it is
 * frozen.
 */
public class UDTColumnDesc extends ComplexColumnDesc {

  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  public final CqlIdentifier udtName;

  private final Map<CqlIdentifier, ColumnDesc> fieldsDesc;

  private final boolean frozen;

  /**
   * UDTColumnDesc constructor. This could be constructed from JSON or CQL.
   *
   * <p>When from user command json. E.G. createTable with UDT column, fieldsDesc is empty, as we
   * don't know the fields yet. See {@link FromJsonFactory}
   */
  public UDTColumnDesc(
      CqlIdentifier udtName, Map<CqlIdentifier, ColumnDesc> fieldsDesc, boolean frozen) {
    super(
        ApiTypeName.UDT,
        ApiSupportDesc.withoutCqlDefinition(
            frozen ? ApiUdtType.API_SUPPORT_FROZEN_UDT : ApiUdtType.API_SUPPORT_NON_FROZEN_UDT));
    this.udtName = Objects.requireNonNull(udtName, "udtName must not be null");
    this.fieldsDesc = Objects.requireNonNull(fieldsDesc, "fieldsDesc must not be null");
    this.frozen = frozen;
  }

  public static class FromJsonFactory extends DescFromJsonFactory {
    FromJsonFactory() {}

    public UDTColumnDesc create(String udtName, boolean frozen) {
      if (udtName == null || udtName.isEmpty()) {
        throw SchemaException.Code.INVALID_USER_DEFINED_TYPE_NAME.get(
            Map.of("typeName", udtName == null ? "null" : udtName));
      }
      // Note, since this is FromJsonFactory. we only know the UDT name from user input, not all the
      // fields description.
      // E.G. when creating a table, we only know the UDT name, not the fields.
      return new UDTColumnDesc(
          CqlIdentifierUtil.cqlIdentifierFromUserInput(udtName), Map.of(), frozen);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var udtColumnDesc = (UDTColumnDesc) o;
    return Objects.equals(udtName, udtColumnDesc.udtName)
        && Objects.equals(fieldsDesc, udtColumnDesc.fieldsDesc)
        && frozen == udtColumnDesc.frozen;
  }

  @Override
  public int hashCode() {
    return Objects.hash(udtName, fieldsDesc, frozen);
  }

  public CqlIdentifier udtName() {
    return udtName;
  }

  public Map<CqlIdentifier, ColumnDesc> fieldsDesc() {
    return fieldsDesc;
  }

  public boolean isFrozen() {
    return frozen;
  }
}
