package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines the API data types that are supported by the API. */
public abstract class ApiDataTypeDefs {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiDataTypeDefs.class);

  // text types
  public static final PrimitiveApiDataTypeDef ASCII =
      new PrimitiveApiDataTypeDef(ApiTypeName.ASCII, DataTypes.ASCII);

  public static final PrimitiveApiDataTypeDef TEXT =
      new PrimitiveApiDataTypeDef(ApiTypeName.TEXT, DataTypes.TEXT);

  // Numeric types
  public static final PrimitiveApiDataTypeDef BIGINT =
      new PrimitiveApiDataTypeDef(ApiTypeName.BIGINT, DataTypes.BIGINT);
  public static final PrimitiveApiDataTypeDef DECIMAL =
      new PrimitiveApiDataTypeDef(ApiTypeName.DECIMAL, DataTypes.DECIMAL);
  public static final PrimitiveApiDataTypeDef DOUBLE =
      new PrimitiveApiDataTypeDef(ApiTypeName.DOUBLE, DataTypes.DOUBLE);
  public static final PrimitiveApiDataTypeDef FLOAT =
      new PrimitiveApiDataTypeDef(ApiTypeName.FLOAT, DataTypes.FLOAT);
  public static final PrimitiveApiDataTypeDef INT =
      new PrimitiveApiDataTypeDef(ApiTypeName.INT, DataTypes.INT);
  public static final PrimitiveApiDataTypeDef SMALLINT =
      new PrimitiveApiDataTypeDef(ApiTypeName.SMALLINT, DataTypes.SMALLINT);
  public static final PrimitiveApiDataTypeDef TINYINT =
      new PrimitiveApiDataTypeDef(ApiTypeName.TINYINT, DataTypes.TINYINT);
  public static final PrimitiveApiDataTypeDef VARINT =
      new PrimitiveApiDataTypeDef(ApiTypeName.VARINT, DataTypes.VARINT);

  // Date and Time types
  public static final PrimitiveApiDataTypeDef DATE =
      new PrimitiveApiDataTypeDef(ApiTypeName.DATE, DataTypes.DATE);

  public static final PrimitiveApiDataTypeDef DURATION =
      new PrimitiveApiDataTypeDef(ApiTypeName.DURATION, DataTypes.DURATION);

  public static final PrimitiveApiDataTypeDef TIME =
      new PrimitiveApiDataTypeDef(ApiTypeName.TIME, DataTypes.TIME);

  public static final PrimitiveApiDataTypeDef TIMESTAMP =
      new PrimitiveApiDataTypeDef(ApiTypeName.TIMESTAMP, DataTypes.TIMESTAMP);

  // Remaining types
  public static final PrimitiveApiDataTypeDef BINARY =
      new PrimitiveApiDataTypeDef(ApiTypeName.BINARY, DataTypes.BLOB);

  public static final PrimitiveApiDataTypeDef BOOLEAN =
      new PrimitiveApiDataTypeDef(ApiTypeName.BOOLEAN, DataTypes.BOOLEAN);

  public static final PrimitiveApiDataTypeDef INET =
      new PrimitiveApiDataTypeDef(ApiTypeName.INET, DataTypes.INET);

  public static final PrimitiveApiDataTypeDef TIMEUUID =
      new PrimitiveApiDataTypeDef(ApiTypeName.TIMEUUID, DataTypes.TIMEUUID);

  public static final PrimitiveApiDataTypeDef UUID =
      new PrimitiveApiDataTypeDef(ApiTypeName.UUID, DataTypes.UUID);

  // Collections use to help lookups, all external access should be through the from() functions
  // below.
  static final List<PrimitiveApiDataTypeDef> PRIMITIVE_TYPES =
      List.of(
          ASCII, BIGINT, BOOLEAN, BINARY, DATE, DECIMAL, DOUBLE, DURATION, FLOAT, INT, SMALLINT,
          TEXT, TIME, TIMESTAMP, TINYINT, VARINT, INET, UUID, TIMEUUID);
}
