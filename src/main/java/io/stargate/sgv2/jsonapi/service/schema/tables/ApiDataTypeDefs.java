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
  public static final ApiDataTypeDef TEXT =
      new ApiDataTypeDef(PrimitiveApiDataType.TEXT, DataTypes.TEXT);

  // Numeric types
  public static final ApiDataTypeDef BIGINT =
      new ApiDataTypeDef(PrimitiveApiDataType.BIGINT, DataTypes.BIGINT);
  public static final ApiDataTypeDef DECIMAL =
      new ApiDataTypeDef(PrimitiveApiDataType.DECIMAL, DataTypes.DECIMAL);
  public static final ApiDataTypeDef DOUBLE =
      new ApiDataTypeDef(PrimitiveApiDataType.DOUBLE, DataTypes.DOUBLE);
  public static final ApiDataTypeDef FLOAT =
      new ApiDataTypeDef(PrimitiveApiDataType.FLOAT, DataTypes.FLOAT);
  public static final ApiDataTypeDef INT =
      new ApiDataTypeDef(PrimitiveApiDataType.INT, DataTypes.INT);
  public static final ApiDataTypeDef SMALLINT =
      new ApiDataTypeDef(PrimitiveApiDataType.SMALLINT, DataTypes.SMALLINT);
  public static final ApiDataTypeDef TINYINT =
      new ApiDataTypeDef(PrimitiveApiDataType.TINYINT, DataTypes.TINYINT);
  public static final ApiDataTypeDef VARINT =
      new ApiDataTypeDef(PrimitiveApiDataType.VARINT, DataTypes.VARINT);

  // Boolean type
  public static final ApiDataTypeDef BOOLEAN =
      new ApiDataTypeDef(PrimitiveApiDataType.BOOLEAN, DataTypes.BOOLEAN);

  public static final ApiDataTypeDef BINARY =
      new ApiDataTypeDef(PrimitiveApiDataType.BINARY, DataTypes.BLOB);

  public static final ApiDataTypeDef DATE =
      new ApiDataTypeDef(PrimitiveApiDataType.DATE, DataTypes.DATE);
  public static final ApiDataTypeDef DURATION =
      new ApiDataTypeDef(PrimitiveApiDataType.DURATION, DataTypes.DURATION);

  public static final ApiDataTypeDef TIME =
      new ApiDataTypeDef(PrimitiveApiDataType.TIME, DataTypes.TIME);

  public static final ApiDataTypeDef TIMESTAMP =
      new ApiDataTypeDef(PrimitiveApiDataType.TIMESTAMP, DataTypes.TIMESTAMP);

  public static final ApiDataTypeDef ASCII =
      new ApiDataTypeDef(PrimitiveApiDataType.ASCII, DataTypes.ASCII);

  // Primitive Types
  public static final List<ApiDataTypeDef> PRIMITIVE_TYPES =
      List.of(
          ASCII, BIGINT, BOOLEAN, BINARY, DATE, DECIMAL, DOUBLE, DURATION, FLOAT, INT, SMALLINT,
          TEXT, TIME, TIMESTAMP, TINYINT, VARINT);

  // Private to force use of the from() method which returns an Optional
  private static final Map<DataType, ApiDataTypeDef> PRIMITIVE_TYPES_BY_CQL_TYPE =
      PRIMITIVE_TYPES.stream()
          .collect(Collectors.toMap(ApiDataTypeDef::getCqlType, Function.identity()));

  public static Optional<ApiDataTypeDef> from(DataType dataType) {
    return Optional.ofNullable(PRIMITIVE_TYPES_BY_CQL_TYPE.get(dataType));
  }

  // Private to force use of the from() method which returns an Optional
  private static final Map<ApiDataType, ApiDataTypeDef> CQL_TYPES_BY_PRIMITIVE_TYPE =
      PRIMITIVE_TYPES.stream()
          .collect(Collectors.toMap(ApiDataTypeDef::getApiType, Function.identity()));

  public static Optional<ApiDataTypeDef> from(ApiDataType dataType) {
    return Optional.ofNullable(CQL_TYPES_BY_PRIMITIVE_TYPE.get(dataType));
  }
}
