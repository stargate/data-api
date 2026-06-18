package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.*;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiMapType extends CollectionApiDataType<MapType> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiMapType.class);

  public static final TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  public static final TypeFactoryFromCql<ApiMapType, MapType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  private final PrimitiveApiDataTypeDef keyType;

  /** NO VALIDATION - Testing Only */
  @VisibleForTesting
  ApiMapType(ApiDataType keyType, ApiDataType valueType, boolean isFrozen) {
    this(
        (PrimitiveApiDataTypeDef) keyType,
        valueType,
        defaultCollectionApiSupport(isFrozen),
        isFrozen);
  }

  private ApiMapType(
      PrimitiveApiDataTypeDef keyType,
      ApiDataType valueType,
      ApiSupportDef apiSupport,
      boolean isFrozen) {
    super(
        ApiTypeName.MAP,
        valueType,
        DataTypes.mapOf(keyType.cqlType(), valueType.cqlType(), isFrozen),
        apiSupport);

    this.keyType = keyType;
  }

  @Override
  public boolean isFrozen() {
    return cqlType.isFrozen();
  }

  @Override
  public ColumnDesc getSchemaDescription(SchemaDescSource schemaDescSource) {
    // no different representation of the map itself, but pass through the binding point
    // because the key or value type may be different, e.g. UDT is the value type
    return new MapColumnDesc(
        schemaDescSource,
        keyType.getSchemaDescription(schemaDescSource),
        valueType.getSchemaDescription(schemaDescSource),
        ApiSupportDesc.from(this));
  }

  public PrimitiveApiDataTypeDef getKeyType() {
    return keyType;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder).append("keyType", keyType);
  }

  /** Factory to create {@link ApiMapType} from user provided {@link MapColumnDesc}. */
  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc> {

    ColumnDescFactory() {
      super(ApiTypeName.MAP, MapColumnDesc.class);
    }

    @Override
    public ApiMapType create(
        TypeBindingPoint bindingPoint,
        MapColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      // can we use a map of any type in this binding point?
      if (!isTypeBindable(bindingPoint, columnDesc, validateVectorize)) {
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.get(
                Map.of(
                    "supportedKeyTypes",
                        errFmtApiTypeName(
                            DefaultTypeFactoryFromColumnDesc.INSTANCE.allBindableTypes(
                                TypeBindingPoint.MAP_KEY)),
                    "supportedValueTypes",
                        errFmtApiTypeName(
                            DefaultTypeFactoryFromColumnDesc.INSTANCE.allBindableTypes(
                                TypeBindingPoint.COLLECTION_VALUE)),
                    "unsupportedKeyType", errFmtOrMissing(columnDesc.keyType()),
                    "unsupportedValueType", errFmtOrMissing(columnDesc.valueType()))));
      }

      try {
        var valueType =
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize);
        var keyType =
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                TypeBindingPoint.MAP_KEY, columnDesc.keyType(), validateVectorize);

        // can never be frozen when created from ColumnDesc / API
        return new ApiMapType(keyType, valueType, false);

      } catch (UnsupportedUserType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedUserType(bindingPoint, columnDesc, e);
      }
    }

    @Override
    public boolean isTypeBindable(
        TypeBindingPoint bindingPoint,
        MapColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      //  we accept frozen, but change the support.

      // can we use a map of any type in this binding point?
      if (!SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromUser()) {
        return false;
      }

      // can the value and key types be used with a map?
      // also, we have to have a key and a value type
      if (columnDesc.valueType() == null
          || !DefaultTypeFactoryFromColumnDesc.INSTANCE.isTypeBindableUntyped(
              TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize)) {
        return false;
      }
      if (columnDesc.keyType() == null
          || !DefaultTypeFactoryFromColumnDesc.INSTANCE.isTypeBindableUntyped(
              TypeBindingPoint.MAP_KEY, columnDesc.keyType(), validateVectorize)) {
        return false;
      }

      return true;
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint) {
      return SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromUser();
    }
  }

  /** Factory to create {@link ApiMapType} from CQL {@link MapType}. */
  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiMapType, MapType> {

    CqlTypeFactory() {
      super(ProtocolConstants.DataType.MAP, MapType.class);
    }

    @Override
    public ApiMapType create(
        TypeBindingPoint bindingPoint, MapType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isTypeBindable(bindingPoint, cqlType)) {
        throw new UnsupportedCqlType(bindingPoint, cqlType);
      }

      try {
        var keyType =
            DefaultTypeFactoryFromCql.INSTANCE.create(
                TypeBindingPoint.MAP_KEY, cqlType.getKeyType(), vectorizeDefn);
        var valueType =
            DefaultTypeFactoryFromCql.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, cqlType.getValueType(), vectorizeDefn);
        return new ApiMapType(keyType, valueType, cqlType.isFrozen());

      } catch (UnsupportedCqlType e) {
        // make sure we have the map type, not just the key or value type
        throw new UnsupportedCqlType(bindingPoint, cqlType, e);
      }
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint, MapType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // we accept frozen and then change the support

      // can we use a map of any type in this binding point?
      if (!SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromDb()) {
        return false;
      }
      // now check if the key and value types are supported for binding into a map
      if (!DefaultTypeFactoryFromCql.INSTANCE.isTypeBindableUntyped(
          TypeBindingPoint.MAP_KEY, cqlType.getKeyType())) {
        return false;
      }
      if (!DefaultTypeFactoryFromCql.INSTANCE.isTypeBindableUntyped(
          TypeBindingPoint.COLLECTION_VALUE, cqlType.getValueType())) {
        return false;
      }
      return true;
    }

    @Override
    public Optional<CqlTypeKey> maybeCreateCacheKey(
        TypeBindingPoint bindingPoint, MapType cqlType) {

      // we can cache the map type if the value type is not a UDT, because the UDT types are tenant
      // specific.
      // we dont support udt's as keys, but checking that anyway so avoid any cross tenant issues
      if (cqlType.getKeyType() instanceof UserDefinedType
          || cqlType.getValueType() instanceof UserDefinedType) {
        return Optional.empty();
      }
      return Optional.of(
          CqlTypeKey.create(
              cqlType, cqlType.getValueType(), cqlType.getKeyType(), cqlType.isFrozen()));
    }
  }
}
