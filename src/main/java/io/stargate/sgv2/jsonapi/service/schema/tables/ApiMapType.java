package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiMapType extends CollectionApiDataType {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiMapType.class);

  public static final TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiMapType, MapType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  private final PrimitiveApiDataTypeDef keyType;

  public ApiMapType(PrimitiveApiDataTypeDef keyType, PrimitiveApiDataTypeDef valueType) {
    super(
        ApiTypeName.MAP,
        valueType,
        DataTypes.mapOf(keyType.cqlType(), valueType.cqlType()),
        new MapColumnDesc(keyType.columnDesc(), valueType.columnDesc()));

    this.keyType = keyType;
    // sanity checking
    if (!isKeyTypeSupported(keyType)) {
      throw new IllegalArgumentException("keyType is not supported");
    }
    if (!isValueTypeSupported(valueType)) {
      throw new IllegalArgumentException("valueType is not supported");
    }
  }

  public static io.stargate.sgv2.jsonapi.service.schema.tables.ApiMapType from(
      ApiDataType keyType, ApiDataType valueType) {
    Objects.requireNonNull(keyType, "keyType must not be null");
    Objects.requireNonNull(valueType, "valueType must not be null");

    if (isKeyTypeSupported(keyType) && isValueTypeSupported(valueType)) {
      return new io.stargate.sgv2.jsonapi.service.schema.tables.ApiMapType(
          (PrimitiveApiDataTypeDef) keyType, (PrimitiveApiDataTypeDef) valueType);
    }
    throw new IllegalArgumentException(
        "keyType and valueType must be primitive types, keyType: %s valueType: %s"
            .formatted(keyType, valueType));
  }

  public static boolean isKeyTypeSupported(ApiDataType keyType) {
    Objects.requireNonNull(keyType, "keyType must not be null");

    // keys must be text or ascii, because keys in JSON are string
    return keyType == ApiDataTypeDefs.ASCII || keyType == ApiDataTypeDefs.TEXT;
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");

    return valueType.isPrimitive();
  }

  public PrimitiveApiDataTypeDef getKeyType() {
    return keyType;
  }

  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc> {

    @Override
    public ApiMapType create(
        String fieldName, MapColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      ApiDataType keyType;
      ApiDataType valueType;
      try {
        keyType =
            TypeFactoryFromColumnDesc.DEFAULT.create(
                fieldName, columnDesc.keyType(), validateVectorize);
        valueType =
            TypeFactoryFromColumnDesc.DEFAULT.create(
                fieldName, columnDesc.valueType(), validateVectorize);
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserType(columnDesc, e);
      }
      // does not call isSupported to avoid two conversions for the key and value types
      if (isKeyTypeSupported(keyType) && isValueTypeSupported(valueType)) {
        return ApiMapType.from(keyType, valueType);
      }
      throw new UnsupportedUserType(columnDesc);
    }

    @Override
    public boolean isSupported(
        MapColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      try {
        return isKeyTypeSupported(
                TypeFactoryFromColumnDesc.DEFAULT.create(
                    null, columnDesc.keyType(), validateVectorize))
            && isValueTypeSupported(
                TypeFactoryFromColumnDesc.DEFAULT.create(
                    null, columnDesc.valueType(), validateVectorize));
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

      if (!isSupported(cqlType)) {
        throw new UnsupportedCqlType(cqlType);
      }

      try {
        return ApiMapType.from(
            TypeFactoryFromCql.DEFAULT.create(cqlType.getKeyType(), vectorizeDefn),
            TypeFactoryFromCql.DEFAULT.create(cqlType.getValueType(), vectorizeDefn));
      } catch (UnsupportedCqlType e) {
        // make sure we have the map type, not just the key or value type
        throw new UnsupportedCqlType(cqlType, e);
      }
    }

    @Override
    public boolean isSupported(MapType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");
      // cannot be frozen
      if (cqlType.isFrozen()) {
        return false;
      }
      // keys must be text or ascii, because keys in JSON are string
      if (!(cqlType.getKeyType() == DataTypes.TEXT || cqlType.getKeyType() == DataTypes.ASCII)) {
        return false;
      }
      // must be a primitive type value
      return cqlType.getValueType() instanceof PrimitiveType;
    }
  }
}
