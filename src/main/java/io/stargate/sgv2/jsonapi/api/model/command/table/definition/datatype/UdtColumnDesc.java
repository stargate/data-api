package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import java.util.Objects;

/**
 * Description of a Udt when used in a column description that includes the full field definition of
 * the UDT. This is created by the {@link ApiUdtType#columnDesc()} when we want to return the full
 * UDT definition as part of an inline schema definition in the read schema response.
 *
 * <p>See also {@link UdtRefColumnDesc}. We extend the column desc because we want to use this type
 * to describe the UDT in both response for full listUdt and inline schema for read commands because
 * they include all the fields.
 *
 * <p>Does not have a factory to create from JSON because it is only created from the ApiUdtType
 */
public class UdtColumnDesc extends UdtRefColumnDesc {

  private final ColumnsDescContainer allFields;

  public UdtColumnDesc(
      SchemaDescSource schemaDescSource, CqlIdentifier udtName, ColumnsDescContainer allFields) {
    this(schemaDescSource, udtName, allFields, null);
  }

  public UdtColumnDesc(
      SchemaDescSource schemaDescSource,
      CqlIdentifier udtName,
      ColumnsDescContainer allFields,
      ApiSupportDesc apiSupportDesc) {
    super(schemaDescSource, udtName, apiSupportDesc);

    this.allFields = Objects.requireNonNull(allFields, "allFields must not be null");
    // sanity check, UDT must have fields
    if (allFields.isEmpty()) {
      throw new IllegalArgumentException("UdtColumnDesc() - allFields is empty");
    }
  }

  public ColumnsDescContainer allFields() {
    return allFields;
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var udtColumnDesc = (UdtColumnDesc) o;
    return Objects.equals(udtName, udtColumnDesc.udtName)
        && Objects.equals(allFields, udtColumnDesc.allFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(udtName(), allFields);
  }
}
