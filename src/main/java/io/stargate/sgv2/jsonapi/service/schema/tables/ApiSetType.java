package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnDesc;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescBindingPoint;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.*;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ApiSetType extends CollectionApiDataType<SetType> {

  public static final TypeFactoryFromColumnDesc<ApiSetType, SetColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  public static final TypeFactoryFromCql<ApiSetType, SetType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  public static final ApiSupportDef API_SUPPORT = defaultApiSupport(false);

  private static final CollectionBindingRules BINDING_POINT_RULES =
      new CollectionBindingRules(
          new CollectionBindingRule(TypeBindingPoint.COLLECTION_VALUE, false),
          new CollectionBindingRule(TypeBindingPoint.MAP_KEY, false),
          new CollectionBindingRule(TypeBindingPoint.TABLE_COLUMN, true),
          new CollectionBindingRule(TypeBindingPoint.UDT_FIELD, false));

  private ApiSetType(ApiDataType valueType, ApiSupportDef apiSupport, boolean isFrozen) {
    super(ApiTypeName.SET, valueType, DataTypes.setOf(valueType.cqlType(), isFrozen), apiSupport);
  }

  @Override
  public boolean isFrozen() {
    return cqlType.isFrozen();
  }

  @Override
  public ColumnDesc getSchemaDescription(SchemaDescBindingPoint bindingPoint) {
    // no different representation of the set itself, but pass through the binding point
    // because the key or value type may be different, e.g. UDT is the value type
    return new SetColumnDesc(
        valueType.getSchemaDescription(bindingPoint),
        ApiSupportDesc.from(this));
  }

  /**
   * Creates a new {@link ApiSetType} from the given ApiDataType value.
   *
   * <p>ApiSupport for valueType should be already validated.
   */
  static ApiSetType from(ApiDataType valueType, boolean isFrozen) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    if (isValueTypeSupported(valueType)) {
      return new ApiSetType(valueType, defaultApiSupport(isFrozen), isFrozen);
    }
    throw new IllegalArgumentException(
        "valueType is not supported, valueType%s".formatted(valueType));
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType.apiSupport().collection().asSetValue();
  }

  /**
   * Factory to create {@link ApiSetType} from {@link SetColumnDesc} obtained from the user.
   *
   * <p>...
   */
  private static final class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiSetType, SetColumnDesc> {

    private ColumnDescFactory() {
      super(ApiTypeName.SET, SetColumnDesc.class);
    }

    @Override
    public ApiSetType create(
        TypeBindingPoint bindingPoint,
        SetColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      if (!isSupported(bindingPoint, columnDesc, validateVectorize)) {
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.UNSUPPORTED_SET_DEFINITION.get(
                Map.of(
                    "supportedTypes",
                    errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs()),
                    "unsupportedValueType",
                    errFmt(columnDesc.valueType()))));
      }

      try {
        var valueType =
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize);
        // can never be frozen when created from ColumnDesc / API
        return ApiSetType.from(valueType, false);

      } catch (UnsupportedUserType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedUserType(bindingPoint, columnDesc, e);
      }
    }

    @Override
    public boolean isSupported(
        TypeBindingPoint bindingPoint,
        SetColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      //  we accept frozen, but change the support.

      // can we use a set of any type in this binding point?
      if (!BINDING_POINT_RULES.forBindingPoint(bindingPoint).isSupported()) {
        return false;
      }

      // can the value type be used as set value?
      if (!DefaultTypeFactoryFromColumnDesc.INSTANCE.isSupportedUntyped(
          TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize)) {
        return false;
      }
      return true;
    }
  }

  /**
   * Factory to create {@link ApiSetType} from {@link SetType} obtained from driver / cql.
   *
   * <p>...
   */
  private static final class CqlTypeFactory extends TypeFactoryFromCql<ApiSetType, SetType> {

    private CqlTypeFactory() {
      super(ApiTypeName.SET, SetType.class);
    }

    @Override
    public ApiSetType create(
        TypeBindingPoint bindingPoint, SetType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(bindingPoint, cqlType)) {
        throw new UnsupportedCqlType(bindingPoint, cqlType);
      }

      try {
        var valueType =
            DefaultTypeFactoryFromCql.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, cqlType.getElementType(), vectorizeDefn);
        return ApiSetType.from(valueType, cqlType.isFrozen());

      } catch (UnsupportedCqlType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedCqlType(bindingPoint, cqlType, e);
      }
    }

    @Override
    public boolean isSupported(TypeBindingPoint bindingPoint, SetType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      //  we accept frozen, but change the support.

      // can we use a set of any type in this binding point?
      if (!BINDING_POINT_RULES.forBindingPoint(bindingPoint).isSupported()) {
        return false;
      }

      // can the value type be used as set value?
      if (!DefaultTypeFactoryFromCql.INSTANCE.isSupportedUntyped(
          TypeBindingPoint.COLLECTION_VALUE, cqlType.getElementType())) {
        return false;
      }
      return true;
    }

    @Override
    public Optional<CqlTypeKey> maybeCreateCacheKey(
        TypeBindingPoint bindingPoint, SetType cqlType) {

      // we can cache the set type if the value type is not a UDT, because the UDT types are tenant
      // specific.
      if (cqlType.getElementType() instanceof UserDefinedType) {
        return Optional.empty();
      }
      return Optional.of(
          CqlTypeKey.create(cqlType, cqlType.getElementType(), null, cqlType.isFrozen()));
    }
  }
}
