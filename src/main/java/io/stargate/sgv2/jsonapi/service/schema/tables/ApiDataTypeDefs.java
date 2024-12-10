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
      new PrimitiveApiDataTypeDef(ApiTypeName.ASCII, DataTypes.ASCII, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef TEXT =
      new PrimitiveApiDataTypeDef(ApiTypeName.TEXT, DataTypes.TEXT, ApiSupportDef.Support.FULL);

  // Numeric types
  public static final PrimitiveApiDataTypeDef BIGINT =
      new PrimitiveApiDataTypeDef(ApiTypeName.BIGINT, DataTypes.BIGINT, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef DECIMAL =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.DECIMAL, DataTypes.DECIMAL, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef DOUBLE =
      new PrimitiveApiDataTypeDef(ApiTypeName.DOUBLE, DataTypes.DOUBLE, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef FLOAT =
      new PrimitiveApiDataTypeDef(ApiTypeName.FLOAT, DataTypes.FLOAT, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef INT =
      new PrimitiveApiDataTypeDef(ApiTypeName.INT, DataTypes.INT, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef SMALLINT =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.SMALLINT, DataTypes.SMALLINT, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef TINYINT =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.TINYINT, DataTypes.TINYINT, ApiSupportDef.Support.FULL);
  public static final PrimitiveApiDataTypeDef VARINT =
      new PrimitiveApiDataTypeDef(ApiTypeName.VARINT, DataTypes.VARINT, ApiSupportDef.Support.FULL);

  // Date and Time types
  public static final PrimitiveApiDataTypeDef DATE =
      new PrimitiveApiDataTypeDef(ApiTypeName.DATE, DataTypes.DATE, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef DURATION =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.DURATION, DataTypes.DURATION, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef TIME =
      new PrimitiveApiDataTypeDef(ApiTypeName.TIME, DataTypes.TIME, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef TIMESTAMP =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.TIMESTAMP, DataTypes.TIMESTAMP, ApiSupportDef.Support.FULL);

  // Remaining types
  public static final PrimitiveApiDataTypeDef BINARY =
      new PrimitiveApiDataTypeDef(ApiTypeName.BINARY, DataTypes.BLOB, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef BOOLEAN =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.BOOLEAN, DataTypes.BOOLEAN, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef COUNTER =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.COUNTER,
          DataTypes.COUNTER,
          new ApiSupportDef.Support(false, false, true, true));

  public static final PrimitiveApiDataTypeDef INET =
      new PrimitiveApiDataTypeDef(ApiTypeName.INET, DataTypes.INET, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef TIMEUUID =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.TIMEUUID,
          DataTypes.TIMEUUID,
          new ApiSupportDef.Support(false, true, true, true));

  public static final PrimitiveApiDataTypeDef UUID =
      new PrimitiveApiDataTypeDef(ApiTypeName.UUID, DataTypes.UUID, ApiSupportDef.Support.FULL);

  // Collections use to help lookups, all external access should be through the from() functions
  // below.
  static final List<PrimitiveApiDataTypeDef> PRIMITIVE_TYPES =
      List.of(
          ASCII, BIGINT, BOOLEAN, BINARY, COUNTER, DATE, DECIMAL, DOUBLE, DURATION, FLOAT, INT,
          SMALLINT, TEXT, TIME, TIMESTAMP, TINYINT, VARINT, INET, UUID, TIMEUUID);
}
