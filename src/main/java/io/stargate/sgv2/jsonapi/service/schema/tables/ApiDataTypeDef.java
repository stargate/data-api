package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Objects;

/**
 * The definition of a type the API supports for a table column.
 *
 * <p>Singleton instances are created in {@link ApiDataTypeDefs} for the supported types.
 *
 * <p>The {@link PrimitiveApiDataType} is the names of the types, this class is used to define how
 * the type works and defines the de/serialisation of the type.
 *
 * <p>aaron - 9 sept 2024 - avoiding a record for now as assume will use subclasses for collections
 */
@JsonSerialize(using = ApiDataTypeDefSerializer.class)
public class ApiDataTypeDef {

  private final ApiDataType apiType;
  private final DataType cqlType;

  public ApiDataTypeDef(ApiDataType apiType, DataType cqlType) {
    this.apiType = apiType;
    this.cqlType = cqlType;
  }

  public ApiDataType getApiType() {
    return apiType;
  }

  public DataType getCqlType() {
    return cqlType;
  }

  public boolean isPrimitive() {
    return !(cqlType instanceof CustomType || cqlType instanceof ContainerType);
  }

  public boolean isContainer() {
    return cqlType instanceof ContainerType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApiDataTypeDef that = (ApiDataTypeDef) o;
    return apiType == that.apiType && Objects.equals(cqlType, that.cqlType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiType, cqlType);
  }

  @Override
  public String toString() {
    return String.format("ApiDataTypeDef{apiType=%s, cqlType=%s}", apiType, cqlType);
  }
}
