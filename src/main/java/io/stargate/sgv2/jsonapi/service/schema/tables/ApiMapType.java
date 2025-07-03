package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.*;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ApiMapType extends CollectionApiDataType<MapType> {

  public static final TypeFactoryFromColumnDesc<ApiMapType, MapColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  public static final TypeFactoryFromCql<ApiMapType, MapType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  private static final CollectionBindingRules BINDING_POINT_RULES =
      new CollectionBindingRules(
          new CollectionBindingRule(TypeBindingPoint.COLLECTION_VALUE, false),
          new CollectionBindingRule(TypeBindingPoint.MAP_KEY, false),
          new CollectionBindingRule(TypeBindingPoint.TABLE_COLUMN, true),
          new CollectionBindingRule(TypeBindingPoint.UDT_FIELD, false));

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  public static final ApiSupportDef API_SUPPORT = defaultApiSupport(false);

  private final PrimitiveApiDataTypeDef keyType;

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
  public ColumnDesc columnDesc() {
    return new MapColumnDesc(
        keyType.columnDesc(), valueType.columnDesc(), ApiSupportDesc.from(this));
  }

  /**
   * Creates a new {@link ApiMapType} from the given ApiDataType key and ApiDataType types.
   *
   * <p>ApiSupport for keyType and valueType should be already validated.
   *
   * <p>As of June 26th, 2025, API only support primitive key types, so UDT can not be used as keys.
   */
  static ApiMapType from(ApiDataType keyType, ApiDataType valueType, boolean isFrozen) {
    Objects.requireNonNull(keyType, "keyType must not be null");
    Objects.requireNonNull(valueType, "valueType must not be null");

    return new ApiMapType(
        (PrimitiveApiDataTypeDef) keyType, valueType, defaultApiSupport(isFrozen), isFrozen);
  }

  //  public static boolean isKeyTypeSupported(ApiDataType keyType) {
  //    Objects.requireNonNull(keyType, "keyType must not be null");
  //    return keyType.apiSupport().collection().asMapKey();
  //  }
  //
  //  public static boolean isValueTypeSupported(ApiDataType valueType) {
  //    Objects.requireNonNull(valueType, "valueType must not be null");
  //    return valueType.apiSupport().collection().asMapValue();
  //  }

  public PrimitiveApiDataTypeDef getKeyType() {
    return keyType;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder).append("keyType", keyType);
  }

  /**
   * Factory to create {@link ApiMapType} from user provided {@link MapColumnDesc}.
   *
   * <p>...
   */
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

      if (!isSupported(bindingPoint, columnDesc, validateVectorize)) {
        // TODO: XXX: AARON: the suported types is prob wrong, it wont include udt as value ?
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.UNSUPPORTED_MAP_DEFINITION.get(
                Map.of(
                    "supportedTypes", errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                    "unsupportedKeyType", errFmt(columnDesc.keyType()),
                    "unsupportedValueType", errFmt(columnDesc.valueType()))));
      }

      try {
        var valueType =
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize);
        var keyType =
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                TypeBindingPoint.MAP_KEY, columnDesc.keyType(), validateVectorize);

        // can never be frozen when created from ColumnDesc / API
        return ApiMapType.from(keyType, valueType, false);

      } catch (UnsupportedUserType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedUserType(bindingPoint, columnDesc, e);
      }
    }

    @Override
    public boolean isSupported(
        TypeBindingPoint bindingPoint,
        MapColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      //  we accept frozen, but change the support.

      // can we use a map of any type in this binding point?
      if (!BINDING_POINT_RULES.forBindingPoint(bindingPoint).isSupported()) {
        return false;
      }

      // can the value and key types be used with a map?
      if (!DefaultTypeFactoryFromColumnDesc.INSTANCE.isSupportedUntyped(
          TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize)) {
        return false;
      }
      if (!DefaultTypeFactoryFromColumnDesc.INSTANCE.isSupportedUntyped(
          TypeBindingPoint.MAP_KEY, columnDesc.keyType(), validateVectorize)) {
        return false;
      }
      return true;
    }
  }

  /**
   * Factory to create {@link ApiMapType} from CQL {@link MapType}.
   *
   * <p>...
   */
  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiMapType, MapType> {

    CqlTypeFactory() {
      super(ApiTypeName.MAP, MapType.class);
    }

    @Override
    public ApiMapType create(
        TypeBindingPoint bindingPoint, MapType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(bindingPoint, cqlType)) {
        throw new UnsupportedCqlType(bindingPoint, cqlType);
      }

      try {
        var keyType =
            DefaultTypeFactoryFromCql.INSTANCE.create(
                TypeBindingPoint.MAP_KEY, cqlType.getKeyType(), vectorizeDefn);
        var valueType =
            DefaultTypeFactoryFromCql.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, cqlType.getValueType(), vectorizeDefn);
        return ApiMapType.from(keyType, valueType, cqlType.isFrozen());

      } catch (UnsupportedCqlType e) {
        // make sure we have the map type, not just the key or value type
        throw new UnsupportedCqlType(bindingPoint, cqlType, e);
      }
    }

    @Override
    public boolean isSupported(TypeBindingPoint bindingPoint, MapType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // we accept frozen and then change the support

      // can we use a map of any type in this binding point?
      if (!BINDING_POINT_RULES.forBindingPoint(bindingPoint).isSupported()) {
        return false;
      }
      // now check if the key and value types are supported for binding into a map
      if (!DefaultTypeFactoryFromCql.INSTANCE.isSupportedUntyped(
          TypeBindingPoint.MAP_KEY, cqlType.getKeyType())) {
        return false;
      }
      if (!DefaultTypeFactoryFromCql.INSTANCE.isSupportedUntyped(
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
