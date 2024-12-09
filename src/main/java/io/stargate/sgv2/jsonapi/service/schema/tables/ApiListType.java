package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ListColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiListType extends CollectionApiDataType {

  public static final TypeFactoryFromColumnDesc<ApiListType, ListColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiListType, ListType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  public static final ApiSupportDef API_SUPPORT = CollectionApiDataType.DEFAULT_API_SUPPORT;

  private ApiListType(PrimitiveApiDataTypeDef valueType) {
    super(ApiTypeName.LIST, valueType, DataTypes.listOf(valueType.cqlType()), API_SUPPORT);
  }

  @Override
  public ColumnDesc columnDesc() {
    return new ListColumnDesc(valueType.columnDesc(), ApiSupportDesc.from(this));
  }

  public static ApiListType from(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");

    if (isValueTypeSupported(valueType)) {
      return new ApiListType((PrimitiveApiDataTypeDef) valueType);
    }

    throw new IllegalArgumentException(
        "valueType must be primitive type, valueType%s".formatted(valueType));
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType.isPrimitive();
  }

  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiListType, ListColumnDesc> {

    @Override
    public ApiListType create(ListColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      ApiDataType valueType;
      try {
        valueType =
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize);
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserType(columnDesc, e);
      }
      // Not calling isSupported to avoid double decoding of the valueType
      if (isValueTypeSupported(valueType)) {
        return ApiListType.from(valueType);
      }
      throw new UnsupportedUserType(columnDesc);
    }

    @Override
    public boolean isSupported(
        ListColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      try {
        return isValueTypeSupported(
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }
  }

  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiListType, ListType> {

    @Override
    public ApiListType create(ListType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(cqlType)) {
        throw new UnsupportedCqlType(cqlType);
      }

      try {
        return ApiListType.from(
            TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn));
      } catch (UnsupportedCqlType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedCqlType(cqlType, e);
      }
    }

    @Override
    public boolean isSupported(ListType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // cannot be frozen
      if (cqlType.isFrozen()) {
        return false;
      }
      // must be a primitive type value
      return cqlType.getElementType() instanceof PrimitiveType;
    }
  }
}
