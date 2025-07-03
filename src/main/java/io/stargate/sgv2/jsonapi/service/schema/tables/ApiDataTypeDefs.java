package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.*;
import java.util.*;
import java.util.function.Predicate;
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
          // Does not support duration as map key. But, duration can be map/list/set value
          ApiTypeName.DURATION,
          DataTypes.DURATION,
          new ApiSupportDef.Support(
              true,
              new ApiSupportDef.Collection(true, true, false, true),
              true,
              true,
              true,
              ApiSupportDef.Update.PRIMITIVE));

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
          // Does not support counter as primitive column, list/set value or map key/value.
          new ApiSupportDef.Support(
              false,
              ApiSupportDef.Collection.NONE,
              false,
              true,
              true,
              ApiSupportDef.Update.PRIMITIVE));

  public static final PrimitiveApiDataTypeDef INET =
      new PrimitiveApiDataTypeDef(ApiTypeName.INET, DataTypes.INET, ApiSupportDef.Support.FULL);

  public static final PrimitiveApiDataTypeDef TIMEUUID =
      new PrimitiveApiDataTypeDef(
          ApiTypeName.TIMEUUID,
          DataTypes.TIMEUUID,
          // Does not support counter as primitive column, list/set value or map key/value.
          new ApiSupportDef.Support(
              false,
              ApiSupportDef.Collection.NONE,
              true,
              true,
              true,
              ApiSupportDef.Update.PRIMITIVE));

  public static final PrimitiveApiDataTypeDef UUID =
      new PrimitiveApiDataTypeDef(ApiTypeName.UUID, DataTypes.UUID, ApiSupportDef.Support.FULL);

  // Collections use to help lookups, all external access should be through the
  // filterBySupportToList() functions
  // below.
  public static final List<PrimitiveApiDataTypeDef> PRIMITIVE_TYPES =
      List.of(
          ASCII, BIGINT, BOOLEAN, BINARY, COUNTER, DATE, DECIMAL, DOUBLE, DURATION, FLOAT, INT,
          SMALLINT, TEXT, TIME, TIMESTAMP, TINYINT, VARINT, INET, UUID, TIMEUUID);

  /**
   * This static method is to filter out all the {@link PrimitiveApiDataTypeDef} that check the
   * given ApiSupport matcher.
   */
  public static List<PrimitiveApiDataTypeDef> filterBySupportToList(
      Predicate<ApiSupportDef> matcher) {
    return PRIMITIVE_TYPES.stream().filter(type -> matcher.test(type.apiSupport())).toList();
  }
}
