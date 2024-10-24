package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnTypes;
import java.util.Objects;

/**
 * The definition of a type the API supports for a table column.
 *
 * <p>Singleton instances are created in {@link ApiDataTypeDefs} for the supported types.
 *
 * <p>The {@link ApiDataTypeName} is the names of the types, this class is used to define how the
 * type works and defines the de/serialisation of the type.
 *
 * <p>aaron - 9 sept 2024 - avoiding a record for now as assume will use subclasses for collections
 */
public class PrimitiveApiDataTypeDef implements ApiDataType {

  private final ApiDataTypeName typeName;
  private final DataType cqlType;

  public PrimitiveApiDataTypeDef(ApiDataTypeName typeName, DataType cqlType) {
    this.typeName = typeName;
    this.cqlType = cqlType;
  }

  @Override
  public ApiDataTypeName getName() {
    return typeName;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean isPrimitive() {
    return true;
  }

  @Override
  public boolean isContainer() {
    return false;
  }

  @Override
  public ColumnType getColumnType() {
    // Not easy to cache in the ctor because of the circular dependency
    // is only a cache lookup so not a big deal
    return PrimitiveColumnTypes.fromApiDataType(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimitiveApiDataTypeDef that = (PrimitiveApiDataTypeDef) o;
    return Objects.equals(typeName, that.typeName) && Objects.equals(cqlType, that.cqlType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, cqlType);
  }

  @Override
  public String toString() {
    return String.format("PrimitiveApiDataTypeDef{typeName=%s, cqlType=%s}", typeName, cqlType);
  }
}
