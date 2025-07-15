package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiTypeName;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
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

public class ApiListType extends CollectionApiDataType<ListType> {

  public static final TypeFactoryFromColumnDesc<ApiListType, ListColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  public static final TypeFactoryFromCql<ApiListType, ListType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  /**
   * Creates a new {@link ApiListType} from the given ApiDataType value.
   *
   * <p>NO VALIDATION - Testing Only
   */
  @VisibleForTesting
  ApiListType(ApiDataType valueType, boolean isFrozen) {
    this(valueType, defaultCollectionApiSupport(isFrozen), isFrozen);
  }

  private ApiListType(ApiDataType valueType, ApiSupportDef apiSupport, boolean isFrozen) {
    super(ApiTypeName.LIST, valueType, DataTypes.listOf(valueType.cqlType(), isFrozen), apiSupport);
  }

  @Override
  public boolean isFrozen() {
    return cqlType.isFrozen();
  }

  @Override
  public ColumnDesc getSchemaDescription(SchemaDescSource schemaDescSource) {
    // no different representation of the list itself, but pass through the binding point
    // because the key or value type may be different, e.g. UDT is the value type
    return new ListColumnDesc(
        schemaDescSource,
        valueType.getSchemaDescription(schemaDescSource),
        ApiSupportDesc.from(this));
  }

  /**
   * Factory to create {@link ApiListType} from {@link ListColumnDesc} obtained from user
   *
   * <p>...
   */
  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiListType, ListColumnDesc> {

    public ColumnDescFactory() {
      super(ApiTypeName.LIST, ListColumnDesc.class);
    }

    @Override
    public ApiListType create(
        TypeBindingPoint bindingPoint,
        ListColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      if (!isTypeBindable(bindingPoint, columnDesc, validateVectorize)) {
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.UNSUPPORTED_LIST_DEFINITION.get(
                Map.of(
                    "supportedTypes",
                    errFmtApiTypeName(
                        DefaultTypeFactoryFromColumnDesc.INSTANCE.allBindableTypes(
                            TypeBindingPoint.COLLECTION_VALUE)),
                    "unsupportedValueType",
                    errFmtOrMissing(columnDesc.valueType()))));
      }

      try {
        var valueType =
            DefaultTypeFactoryFromColumnDesc.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize);
        // can never be frozen when created from ColumnDesc / API
        return new ApiListType(valueType, false);

      } catch (UnsupportedUserType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedUserType(bindingPoint, columnDesc, e);
      }
    }

    @Override
    public boolean isTypeBindable(
        TypeBindingPoint bindingPoint,
        ListColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      //  we accept frozen, but change the support.

      // can we use a list of any type in this binding point?
      if (!SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromUser()) {
        return false;
      }

      // can the value type be used as list value?
      // also, we have to have a value typ
      if (columnDesc.valueType() == null
          || !DefaultTypeFactoryFromColumnDesc.INSTANCE.isTypeBindableUntyped(
              TypeBindingPoint.COLLECTION_VALUE, columnDesc.valueType(), validateVectorize)) {
        return false;
      }
      return true;
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint) {
      return SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromUser();
    }
  }

  /**
   * Type factory to create {@link ApiListType} from {@link ListType} obtained from driver / cql.
   *
   * <p>...
   */
  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiListType, ListType> {

    public CqlTypeFactory() {
      super(ProtocolConstants.DataType.LIST, ListType.class);
    }

    @Override
    public ApiListType create(
        TypeBindingPoint bindingPoint, ListType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isTypeBindable(bindingPoint, cqlType)) {
        throw new UnsupportedCqlType(bindingPoint, cqlType);
      }

      try {
        var valueType =
            DefaultTypeFactoryFromCql.INSTANCE.create(
                TypeBindingPoint.COLLECTION_VALUE, cqlType.getElementType(), vectorizeDefn);
        return new ApiListType(valueType, cqlType.isFrozen());

      } catch (UnsupportedCqlType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedCqlType(bindingPoint, cqlType, e);
      }
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint, ListType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      //  we accept frozen, but change the support.

      // can we use a list of any type in this binding point?
      if (!SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromDb()) {
        return false;
      }

      // can the value type be used as list value?
      if (!DefaultTypeFactoryFromCql.INSTANCE.isTypeBindableUntyped(
          TypeBindingPoint.COLLECTION_VALUE, cqlType.getElementType())) {
        return false;
      }
      return true;
    }

    @Override
    public Optional<CqlTypeKey> maybeCreateCacheKey(
        TypeBindingPoint bindingPoint, ListType cqlType) {

      // we can cache the list type if the value type is not a UDT, because the UDT types are tenant
      // specific.
      if (cqlType.getElementType() instanceof UserDefinedType) {
        return Optional.empty();
      }
      return Optional.of(
          CqlTypeKey.create(cqlType, cqlType.getElementType(), null, cqlType.isFrozen()));
    }
  }
}
