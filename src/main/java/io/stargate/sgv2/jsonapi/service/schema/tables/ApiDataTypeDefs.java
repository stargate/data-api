package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Defines the API data types that are supported by the API. */
public abstract class ApiDataTypeDefs {

  // text types
  public static final ApiDataTypeDef TEXT = new ApiDataTypeDef(ApiDataType.TEXT, DataTypes.TEXT);

  // Numeric types
  public static final ApiDataTypeDef BIGINT =
      new ApiDataTypeDef(ApiDataType.BIGINT, DataTypes.BIGINT);
  public static final ApiDataTypeDef DECIMAL =
      new ApiDataTypeDef(ApiDataType.DECIMAL, DataTypes.DECIMAL);
  public static final ApiDataTypeDef DOUBLE =
      new ApiDataTypeDef(ApiDataType.DOUBLE, DataTypes.DOUBLE);
  public static final ApiDataTypeDef FLOAT = new ApiDataTypeDef(ApiDataType.FLOAT, DataTypes.FLOAT);
  public static final ApiDataTypeDef INT = new ApiDataTypeDef(ApiDataType.INT, DataTypes.INT);
  public static final ApiDataTypeDef SMALLINT =
      new ApiDataTypeDef(ApiDataType.SMALLINT, DataTypes.SMALLINT);
  public static final ApiDataTypeDef TINYINT =
      new ApiDataTypeDef(ApiDataType.TINYINT, DataTypes.TINYINT);
  public static final ApiDataTypeDef VARINT =
      new ApiDataTypeDef(ApiDataType.VARINT, DataTypes.VARINT);

  // Boolean type
  public static final ApiDataTypeDef BOOLEAN =
      new ApiDataTypeDef(ApiDataType.BOOLEAN, DataTypes.BOOLEAN);

  // Primitive Types
  public static final List<ApiDataTypeDef> PRIMITIVE_TYPES =
      List.of(TEXT, BIGINT, DECIMAL, DOUBLE, FLOAT, INT, SMALLINT, TINYINT, VARINT, BOOLEAN);

  // Private to force use of the from() method which returns an Optional
  private static final Map<DataType, ApiDataTypeDef> PRIMITIVE_TYPES_BY_CQL_TYPE =
      PRIMITIVE_TYPES.stream()
          .collect(Collectors.toMap(ApiDataTypeDef::getCqlType, Function.identity()));

  public static Optional<ApiDataTypeDef> from(DataType dataType) {
    return Optional.ofNullable(PRIMITIVE_TYPES_BY_CQL_TYPE.get(dataType));
  }
}
