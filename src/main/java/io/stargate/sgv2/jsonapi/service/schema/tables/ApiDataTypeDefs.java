package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnType;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Defines the API data types that are supported by the API. */
public abstract class ApiDataTypeDefs {

  // text types
  public static final PrimitiveApiDataTypeDef ASCII =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.ASCII, DataTypes.ASCII);

  public static final PrimitiveApiDataTypeDef TEXT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.TEXT, DataTypes.TEXT);

  // Numeric types
  public static final PrimitiveApiDataTypeDef BIGINT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.BIGINT, DataTypes.BIGINT);
  public static final PrimitiveApiDataTypeDef DECIMAL =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.DECIMAL, DataTypes.DECIMAL);
  public static final PrimitiveApiDataTypeDef DOUBLE =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.DOUBLE, DataTypes.DOUBLE);
  public static final PrimitiveApiDataTypeDef FLOAT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.FLOAT, DataTypes.FLOAT);
  public static final PrimitiveApiDataTypeDef INT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.INT, DataTypes.INT);
  public static final PrimitiveApiDataTypeDef SMALLINT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.SMALLINT, DataTypes.SMALLINT);
  public static final PrimitiveApiDataTypeDef TINYINT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.TINYINT, DataTypes.TINYINT);
  public static final PrimitiveApiDataTypeDef VARINT =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.VARINT, DataTypes.VARINT);

  // Date and Time types
  public static final PrimitiveApiDataTypeDef DATE =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.DATE, DataTypes.DATE);

  public static final PrimitiveApiDataTypeDef DURATION =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.DURATION, DataTypes.DURATION);

  public static final PrimitiveApiDataTypeDef TIME =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.TIME, DataTypes.TIME);

  public static final PrimitiveApiDataTypeDef TIMESTAMP =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.TIMESTAMP, DataTypes.TIMESTAMP);

  // Remaining types
  public static final PrimitiveApiDataTypeDef BINARY =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.BINARY, DataTypes.BLOB);

  public static final PrimitiveApiDataTypeDef BOOLEAN =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.BOOLEAN, DataTypes.BOOLEAN);

  public static final PrimitiveApiDataTypeDef INET =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.INET, DataTypes.INET);

  public static final PrimitiveApiDataTypeDef TIMEUUID =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.TIMEUUID, DataTypes.TIMEUUID);

  public static final PrimitiveApiDataTypeDef UUID =
      new PrimitiveApiDataTypeDef(ApiDataTypeName.UUID, DataTypes.UUID);

  // Collections use to help lookups, all external access should be through the from() functions
  // below.
  private static final List<PrimitiveApiDataTypeDef> PRIMITIVE_TYPES =
      List.of(
          ASCII, BIGINT, BOOLEAN, BINARY, DATE, DECIMAL, DOUBLE, DURATION, FLOAT, INT, SMALLINT,
          TEXT, TIME, TIMESTAMP, TINYINT, VARINT, INET, UUID, TIMEUUID);

  private static final Map<DataType, PrimitiveApiDataTypeDef> PRIMITIVE_TYPES_BY_CQL_TYPE =
      PRIMITIVE_TYPES.stream()
          .collect(Collectors.toMap(PrimitiveApiDataTypeDef::getCqlType, Function.identity()));

  private static final Map<ApiDataTypeName, PrimitiveApiDataTypeDef> PRIMITIVE_TYPES_BY_API_NAME =
      PRIMITIVE_TYPES.stream()
          .collect(Collectors.toMap(PrimitiveApiDataTypeDef::getName, Function.identity()));

  //  private static final Map<ApiDataType, PrimitiveApiDataTypeDef> CQL_TYPES_BY_PRIMITIVE_TYPE =
  //      PRIMITIVE_TYPES.stream()
  //          .collect(Collectors.toMap(PrimitiveApiDataTypeDef::getApiType, Function.identity()));

  // Caching the API complex type by the CQL type so we do not need to create instances for the same
  // CQL type.
  private static final ConcurrentMap<MapTypeCacheKey, ComplexApiDataType.ApiMapType>
      CQL_MAP_TYPE_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentMap<ListType, ComplexApiDataType.ApiListType> CQL_LIST_TYPE_CACHE =
      new ConcurrentHashMap<>();
  private static final ConcurrentMap<SetType, ComplexApiDataType.ApiSetType> CQL_SET_TYPE_CACHE =
      new ConcurrentHashMap<>();

  private static final ConcurrentMap<ComplexColumnType.ColumnMapType, ComplexApiDataType.ApiMapType>
      COL_MAP_TYPE_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentMap<
          ComplexColumnType.ColumnListType, ComplexApiDataType.ApiListType>
      COL_LIST_TYPE_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentMap<ComplexColumnType.ColumnSetType, ComplexApiDataType.ApiSetType>
      COL_SET_TYPE_CACHE = new ConcurrentHashMap<>();

  /**
   * Gets an {@link PrimitiveApiDataTypeDef} for the given CQL {@link DataType}.
   *
   * <p>Handles Primitive and Collection types.
   *
   * @param dataType the CQL data type to get the API data type for.
   * @return The optional of {@link PrimitiveApiDataTypeDef}, if the optional it empty it means the
   *     data type is not supported by the API for any options. If it is present, should still check
   *     the {@link PrimitiveApiDataTypeDef} for the level of support.
   */
  public static ApiDataType from(DataType dataType) throws UnsupportedCqlType {
    Objects.requireNonNull(dataType, "dataType must not be null");

    var primitiveType = PRIMITIVE_TYPES_BY_CQL_TYPE.get(dataType);
    if (primitiveType != null) {
      return primitiveType;
    }

    return switch (dataType) {
      case MapType mt -> from(mt);
      case ListType lt -> from(lt);
      case SetType st -> from(st);
      case VectorType vt -> from(vt);
      default -> throw new UnsupportedCqlType(dataType);
    };
  }

  public static ApiDataType from(ColumnType columnType) throws UnsupportedUserType {
    Objects.requireNonNull(columnType, "columnType must not be null");

    var primitiveType = PRIMITIVE_TYPES_BY_API_NAME.get(columnType.getApiDataTypeName());
    if (primitiveType != null) {
      return primitiveType;
    }

    return switch (columnType) {
      case ComplexColumnType.ColumnListType lt -> from(lt);
      case ComplexColumnType.ColumnMapType mt -> from(mt);
      case ComplexColumnType.ColumnSetType st -> from(st);
      case ComplexColumnType.ColumnVectorType vt -> from(vt);
      default -> throw new UnsupportedUserType(columnType);
    };
  }

  public static ComplexApiDataType from(MapType mapType) throws UnsupportedCqlType {
    Objects.requireNonNull(mapType, "mapType must not be null");

    if (!ComplexApiDataType.ApiMapType.isCqlTypeSupported(mapType)) {
      throw new UnsupportedCqlType(mapType);
    }

    var cacheKey =
        new MapTypeCacheKey(mapType.getKeyType(), mapType.getValueType(), mapType.isFrozen());
    return CQL_MAP_TYPE_CACHE.computeIfAbsent(
        cacheKey,
        entry -> {

          // supported check above should also make sure the key and map types are supported
          // from() will throw if that is not the case.
          try {
            return ComplexApiDataType.ApiMapType.from(
                from(mapType.getKeyType()), from(mapType.getValueType()));
          } catch (UnsupportedCqlType e) {
            // should not happen if the isCqlTypeSupported returns true
            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
          }
        });
  }

  public static ComplexApiDataType from(ComplexColumnType.ColumnMapType mapType)
      throws UnsupportedUserType {
    Objects.requireNonNull(mapType, "mapType must not be null");

    if (!ComplexApiDataType.ApiMapType.isColumnTypeSupported(mapType)) {
      throw new UnsupportedUserType(mapType);
    }

    return COL_MAP_TYPE_CACHE.computeIfAbsent(
        mapType,
        entry -> {
          // supported check above should also make sure the value type are supported
          // from() will throw if that is not the case.
          try {
            return ComplexApiDataType.ApiMapType.from(mapType);
          } catch (UnsupportedUserType e) {
            // should not happen if the isColumnTypeSupported returns true
            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
          }
        });
  }

  public static ComplexApiDataType from(ListType listType) throws UnsupportedCqlType {
    Objects.requireNonNull(listType, "listType must not be null");

    if (!ComplexApiDataType.ApiListType.isCqlTypeSupported(listType)) {
      throw new UnsupportedCqlType(listType);
    }

    return CQL_LIST_TYPE_CACHE.computeIfAbsent(
        listType,
        entry -> {
          // supported check above should also make sure the value type are supported
          // from() will throw if that is not the case.
          try {
            return ComplexApiDataType.ApiListType.from(from(listType.getElementType()));
          } catch (UnsupportedCqlType e) {
            // should not happen if the isCqlTypeSupported returns true
            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
          }
        });
  }

  public static ComplexApiDataType from(ComplexColumnType.ColumnListType listType)
      throws UnsupportedUserType {
    Objects.requireNonNull(listType, "listType must not be null");

    if (!ComplexApiDataType.ApiListType.isColumnTypeSupported(listType)) {
      throw new UnsupportedUserType(listType);
    }

    return COL_LIST_TYPE_CACHE.computeIfAbsent(
        listType,
        entry -> {
          // supported check above should also make sure the value type are supported
          // from() will throw if that is not the case.
          try {
            return ComplexApiDataType.ApiListType.from(listType);
          } catch (UnsupportedUserType e) {
            // should not happen if the isColumnTypeSupported returns true
            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
          }
        });
  }

  public static ComplexApiDataType from(SetType setType) throws UnsupportedCqlType {
    Objects.requireNonNull(setType, "setType must not be null");

    if (!ComplexApiDataType.ApiSetType.isCqlTypeSupported(setType)) {
      throw new UnsupportedCqlType(setType);
    }

    // supported check above should also make sure the value type are supported
    // from() will throw if that is not the case.
    return CQL_SET_TYPE_CACHE.computeIfAbsent(
        setType,
        entry -> {
          try {
            return ComplexApiDataType.ApiSetType.from(from(setType.getElementType()));
          } catch (UnsupportedCqlType e) {
            // should not happen if the isCqlTypeSupported returns true
            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
          }
        });
  }

  public static ComplexApiDataType from(ComplexColumnType.ColumnSetType setType)
      throws UnsupportedUserType {
    Objects.requireNonNull(setType, "setType must not be null");

    if (!ComplexApiDataType.ApiSetType.isColumnTypeSupported(setType)) {
      throw new UnsupportedUserType(setType);
    }

    return COL_SET_TYPE_CACHE.computeIfAbsent(
        setType,
        entry -> {
          // supported check above should also make sure the value type are supported
          // from() will throw if that is not the case.
          try {
            return ComplexApiDataType.ApiSetType.from(setType);
          } catch (UnsupportedUserType e) {
            // should not happen if the isColumnTypeSupported returns true
            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
          }
        });
  }

  /**
   * Note: cannot cache this as the {@link ComplexApiDataType.ApiVectorType} includes the vectorize
   * config so is not re-usable cross tenants etc.
   *
   * @param vectorType
   * @return
   * @throws UnsupportedCqlType
   */
  public static ApiDataType from(VectorType vectorType) throws UnsupportedCqlType {
    Objects.requireNonNull(vectorType, "vectorType must not be null");

    if (!ComplexApiDataType.ApiVectorType.isCqlTypeSupported(vectorType)) {
      throw new UnsupportedCqlType(vectorType);
    }

    try {
      return ComplexApiDataType.ApiVectorType.from(
          from(vectorType.getElementType()), vectorType.getDimensions(), null);
    } catch (UnsupportedCqlType e) {
      // should not happen if the isCqlTypeSupported returns true
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
    }
  }

  /**
   * Note: cannot cache this as the {@link ComplexApiDataType.ApiVectorType} includes the vectorize
   * config so is not re-usable cross tenants etc.
   */
  public static ComplexApiDataType from(ComplexColumnType.ColumnVectorType vectorType)
      throws UnsupportedUserType {
    Objects.requireNonNull(vectorType, "vectorType must not be null");

    if (!ComplexApiDataType.ApiVectorType.isColumnTypeSupported(vectorType)) {
      throw new UnsupportedUserType(vectorType);
    }

    try {
      return ComplexApiDataType.ApiVectorType.from(vectorType);
    } catch (UnsupportedUserType e) {
      // should not happen if the isColumnTypeSupported returns true
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
    }
  }

  /**
   * Custom cache key for the map type cache, because the equals on the {@link
   * com.datastax.oss.driver.internal.core.type.DefaultMapType#equals(Object)} does not take the
   * frozen flag into account.
   *
   * <p>Even though we do not support frozen (See {@link
   * ComplexApiDataType.ApiMapType#isCqlTypeSupported(MapType)} felt saver to include it in the
   * cache key.
   */
  private record MapTypeCacheKey(DataType keyType, DataType valueType, boolean frozen) {}
}
