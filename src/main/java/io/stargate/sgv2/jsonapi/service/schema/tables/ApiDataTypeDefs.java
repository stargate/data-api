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

  //  private static final Map<ApiDataType, PrimitiveApiDataTypeDef> CQL_TYPES_BY_PRIMITIVE_TYPE =
  //      PRIMITIVE_TYPES.stream()
  //          .collect(Collectors.toMap(PrimitiveApiDataTypeDef::getApiType, Function.identity()));

  // Caching the API complex type by the CQL type so we do not need to create instances for the same
  // CQL type.

  //  private static final ConcurrentMap<ComplexColumnDesc.MapColumnDesc, ApiMapType>
  //      COL_MAP_TYPE_CACHE = new ConcurrentHashMap<>();
  //  private static final ConcurrentMap<
  //          ComplexColumnDesc.ListColumnDesc, ApiListType>
  //      COL_LIST_TYPE_CACHE = new ConcurrentHashMap<>();
  //  private static final ConcurrentMap<ComplexColumnDesc.SetColumnDesc, ApiSetType>
  //      COL_SET_TYPE_CACHE = new ConcurrentHashMap<>();

  //  /**
  //   * Gets an {@link PrimitiveApiDataTypeDef} for the given CQL {@link DataType}.
  //   *
  //   * <p>Handles Primitive and Collection types.
  //   *
  //   * @param dataType the CQL data type to get the API data type for.
  //   * @return The optional of {@link PrimitiveApiDataTypeDef}, if the optional it empty it means
  // the
  //   *     data type is not supported by the API for any options. If it is present, should still
  // check
  //   *     the {@link PrimitiveApiDataTypeDef} for the level of support.
  //   */
  //  public static ApiDataType from(DataType dataType) throws UnsupportedCqlType {
  //    Objects.requireNonNull(dataType, "dataType must not be null");
  //
  //    var primitiveType = PRIMITIVE_TYPES_BY_CQL_TYPE.get(dataType);
  //    if (primitiveType != null) {
  //      return primitiveType;
  //    }
  //
  //    return switch (dataType) {
  //      case MapType mt -> from(mt);
  //      case ListType lt -> from(lt);
  //      case SetType st -> from(st);
  //      case VectorType vt -> from(vt);
  //      default -> throw new UnsupportedCqlType(dataType);
  //    };
  //  }

  // HERE
  //  public static ApiDataType from(ColumnDesc columnDesc, VectorizeConfigValidator
  // vectorizeValidator) throws UnsupportedUserType {
  //    Objects.requireNonNull(columnDesc, "columnDesc must not be null");
  //
  //    LOGGER.warn("processing columnDesc: {}", columnDesc);
  //    var primitiveType = PRIMITIVE_TYPES_BY_API_NAME.get(columnDesc.getApiDataTypeName());
  //    if (primitiveType != null) {
  //      return primitiveType;
  //    }
  //
  //    LOGGER.warn(
  //        "processing instance of ListColumnDesc: {}",
  //        columnDesc instanceof ComplexColumnDesc.ListColumnDesc);
  //    LOGGER.warn(
  //        "processing instance of MapColumnDesc: {}",
  //        columnDesc instanceof ComplexColumnDesc.MapColumnDesc);
  //    LOGGER.warn(
  //        "processing instance of SetColumnDesc: {}",
  //        columnDesc instanceof ComplexColumnDesc.SetColumnDesc);
  //    LOGGER.warn(
  //        "processing instance of VectorColumnDesc: {}",
  //        columnDesc instanceof ComplexColumnDesc.VectorColumnDesc);
  //
  //    return switch (columnDesc) {
  //      case ComplexColumnDesc.ListColumnDesc lt -> from(lt);
  //      case ComplexColumnDesc.MapColumnDesc mt -> from(mt);
  //      case ComplexColumnDesc.SetColumnDesc st -> from(st);
  //      case ComplexColumnDesc.VectorColumnDesc vt -> from(vt, vectorizeValidator);
  //      default -> throw new UnsupportedUserType(columnDesc);
  //    };
  //  }

  //  public static CollectionApiDataType from(MapType mapType) throws UnsupportedCqlType {
  //    Objects.requireNonNull(mapType, "mapType must not be null");
  //
  //    if (!ApiMapType.isCqlTypeSupported(mapType)) {
  //      throw new UnsupportedCqlType(mapType);
  //    }
  //
  //    var cacheKey =
  //        new MapTypeCacheKey(mapType.getKeyType(), mapType.getValueType(), mapType.isFrozen());
  //    return CQL_MAP_TYPE_CACHE.computeIfAbsent(
  //        cacheKey,
  //        entry -> {
  //
  //          // supported check above should also make sure the key and map types are supported
  //          // from() will throw if that is not the case.
  //          try {
  //            return ApiMapType.from(
  //                from(mapType.getKeyType()), from(mapType.getValueType()));
  //          } catch (UnsupportedCqlType e) {
  //            // should not happen if the isCqlTypeSupported returns true
  //            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //          }
  //        });
  //  }

  //  public static CollectionApiDataType from(ComplexColumnDesc.MapColumnDesc mapType)
  //      throws UnsupportedUserType {
  //    Objects.requireNonNull(mapType, "mapType must not be null");
  //
  //    if (!ApiMapType.isColumnTypeSupported(mapType)) {
  //      throw new UnsupportedUserType(mapType);
  //    }
  //
  //    return COL_MAP_TYPE_CACHE.computeIfAbsent(
  //        mapType,
  //        entry -> {
  //          // supported check above should also make sure the value type are supported
  //          // from() will throw if that is not the case.
  //          try {
  //            return ApiMapType.from(mapType);
  //          } catch (UnsupportedUserType e) {
  //            // should not happen if the isColumnTypeSupported returns true
  //            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //          }
  //        });
  //  }

  //  public static CollectionApiDataType from(ListType listType) throws UnsupportedCqlType {
  //    Objects.requireNonNull(listType, "listType must not be null");
  //
  //    if (!ApiListType.isCqlTypeSupported(listType)) {
  //      throw new UnsupportedCqlType(listType);
  //    }
  //
  //    return CQL_LIST_TYPE_CACHE.computeIfAbsent(
  //        listType,
  //        entry -> {
  //          // supported check above should also make sure the value type are supported
  //          // from() will throw if that is not the case.
  //          try {
  //            return ApiListType.from(from(listType.getElementType()));
  //          } catch (UnsupportedCqlType e) {
  //            // should not happen if the isCqlTypeSupported returns true
  //            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //          }
  //        });
  //  }

  //  public static CollectionApiDataType from(ComplexColumnDesc.ListColumnDesc listType)
  //      throws UnsupportedUserType {
  //    Objects.requireNonNull(listType, "listType must not be null");
  //
  //    if (!ApiListType.isColumnTypeSupported(listType)) {
  //      throw new UnsupportedUserType(listType);
  //    }
  //
  //    return COL_LIST_TYPE_CACHE.computeIfAbsent(
  //        listType,
  //        entry -> {
  //          // supported check above should also make sure the value type are supported
  //          // from() will throw if that is not the case.
  //          try {
  //            return ApiListType.from(listType);
  //          } catch (UnsupportedUserType e) {
  //            // should not happen if the isColumnTypeSupported returns true
  //            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //          }
  //        });
  //  }

  //  public static CollectionApiDataType from(SetType setType) throws UnsupportedCqlType {
  //    Objects.requireNonNull(setType, "setType must not be null");
  //
  //    if (!ApiSetType.isCqlTypeSupported(setType)) {
  //      throw new UnsupportedCqlType(setType);
  //    }
  //
  //    // supported check above should also make sure the value type are supported
  //    // from() will throw if that is not the case.
  //    return CQL_SET_TYPE_CACHE.computeIfAbsent(
  //        setType,
  //        entry -> {
  //          try {
  //            return ApiSetType.from(from(setType.getElementType()));
  //          } catch (UnsupportedCqlType e) {
  //            // should not happen if the isCqlTypeSupported returns true
  //            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //          }
  //        });
  //  }

  //  public static CollectionApiDataType from(ComplexColumnDesc.SetColumnDesc setType)
  //      throws UnsupportedUserType {
  //    Objects.requireNonNull(setType, "setType must not be null");
  //
  //    if (!ApiSetType.isColumnTypeSupported(setType)) {
  //      throw new UnsupportedUserType(setType);
  //    }
  //
  //    return COL_SET_TYPE_CACHE.computeIfAbsent(
  //        setType,
  //        entry -> {
  //          // supported check above should also make sure the value type are supported
  //          // from() will throw if that is not the case.
  //          try {
  //            return ApiSetType.from(setType);
  //          } catch (UnsupportedUserType e) {
  //            // should not happen if the isColumnTypeSupported returns true
  //            throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //          }
  //        });
  //  }

  //  /**
  //   * Note: cannot cache this as the {@link ApiVectorType} includes the vectorize
  //   * config so is not re-usable cross tenants etc.
  //   */
  //  public static CollectionApiDataType from(ComplexColumnDesc.VectorColumnDesc vectorType)
  //      throws UnsupportedUserType {
  //    Objects.requireNonNull(vectorType, "vectorType must not be null");
  //
  //    if (!ApiVectorType.isColumnTypeSupported(vectorType)) {
  //      throw new UnsupportedUserType(vectorType);
  //    }
  //
  //    try {
  //      return ApiVectorType.from(vectorType);
  //    } catch (UnsupportedUserType e) {
  //      // should not happen if the isColumnTypeSupported returns true
  //      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
  //    }
  //  }

}
