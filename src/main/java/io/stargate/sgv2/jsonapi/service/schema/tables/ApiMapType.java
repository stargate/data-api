package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiMapType extends CollectionApiDataType<MapType> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiMapType.class);

  public static final TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiMapType, MapType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  public static final ApiSupportDef API_SUPPORT = defaultApiSupport(false);

  private final PrimitiveApiDataTypeDef keyType;

  private ApiMapType(
      PrimitiveApiDataTypeDef keyType,
      PrimitiveApiDataTypeDef valueType,
      ApiSupportDef apiSupport,
      boolean isFrozen) {
    super(
        ApiTypeName.MAP,
        valueType,
        DataTypes.mapOf(keyType.cqlType(), valueType.cqlType(), isFrozen),
        apiSupport);

    this.keyType = keyType;
    // sanity checking
    if (!isKeyTypeSupported(keyType)) {
      throw new IllegalArgumentException("keyType is not supported");
    }
    if (!isValueTypeSupported(valueType)) {
      throw new IllegalArgumentException("valueType is not supported");
    }
  }

  @Override
  public boolean isFrozen() {
    return cqlType.isFrozen();
  }

  @Override
  public ColumnDesc columnDesc() {
    return new MapColumnDesc(
        keyType.columnDesc(), valueType.columnDesc(), ApiSupportDesc.from(this));
  }

  /**
   * Creates a new {@link ApiMapType} from the given ApiDataType key and ApiDataType types.
   *
   * <p>ApiSupport for keyType and valueType should be already validated.
   */
  static ApiMapType from(ApiDataType keyType, ApiDataType valueType, boolean isFrozen) {
    Objects.requireNonNull(keyType, "keyType must not be null");
    Objects.requireNonNull(valueType, "valueType must not be null");

    if (isKeyTypeSupported(keyType) && isValueTypeSupported(valueType)) {
      return new ApiMapType(
          (PrimitiveApiDataTypeDef) keyType,
          (PrimitiveApiDataTypeDef) valueType,
          defaultApiSupport(isFrozen),
          isFrozen);
    }
    throw new IllegalArgumentException(
        "keyType or valueType is not supported, keyType: %s valueType: %s"
            .formatted(keyType, valueType));
  }

  public static boolean isKeyTypeSupported(ApiDataType keyType) {
    Objects.requireNonNull(keyType, "keyType must not be null");
    return keyType.apiSupport().collection().asMapKey();
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType.apiSupport().collection().asMapValue();
  }

  public PrimitiveApiDataTypeDef getKeyType() {
    return keyType;
  }

  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc> {

    @Override
    public ApiMapType create(MapColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      ApiDataType keyType;
      ApiDataType valueType;

      try {
        keyType = TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.keyType(), validateVectorize);
        valueType =
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize);
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserType(columnDesc, e);
      }
      // does not call isSupported to avoid two conversions for the key and value types
      if (isKeyTypeSupported(keyType) && isValueTypeSupported(valueType)) {
        // never frozen from the API
        return ApiMapType.from(keyType, valueType, false);
      }
      throw new UnsupportedUserType(columnDesc);
    }

    @Override
    public boolean isSupported(
        MapColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      try {
        return isKeyTypeSupported(
                TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.keyType(), validateVectorize))
            && isValueTypeSupported(
                TypeFactoryFromColumnDesc.DEFAULT.create(
                    columnDesc.valueType(), validateVectorize));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }
  }

  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiMapType, MapType> {

    @Override
    public ApiMapType create(MapType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      try {
        ApiDataType keyType =
            TypeFactoryFromCql.DEFAULT.create(cqlType.getKeyType(), vectorizeDefn);
        if (!isKeyTypeSupported(keyType)) {
          throw new UnsupportedCqlType(cqlType);
        }

        ApiDataType valueType =
            TypeFactoryFromCql.DEFAULT.create(cqlType.getValueType(), vectorizeDefn);
        if (!isValueTypeSupported(valueType)) {
          throw new UnsupportedCqlType(cqlType);
        }

        return ApiMapType.from(keyType, valueType, cqlType.isFrozen());
      } catch (UnsupportedCqlType e) {
        // make sure we have the map type, not just the key or value type
        throw new UnsupportedCqlType(cqlType, e);
      }
    }

    @Override
    public boolean isSupported(MapType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // we accept frozen and then change the support
      // TODO WHY there is vectorizeDefn in the create method?
      try {
        var keyType = TypeFactoryFromCql.DEFAULT.create(cqlType.getKeyType(), null);
        var valueType = TypeFactoryFromCql.DEFAULT.create(cqlType.getValueType(), null);
        return isKeyTypeSupported(keyType) && isValueTypeSupported(valueType);
      } catch (UnsupportedCqlType e) {
        return false;
      }
    }
  }
}
