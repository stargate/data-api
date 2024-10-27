package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiSetType extends CollectionApiDataType {

  public static final TypeFactoryFromColumnDesc<ApiSetType, ComplexColumnDesc.SetColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiSetType, SetType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  private ApiSetType(PrimitiveApiDataTypeDef valueType) {
    super(
        ApiDataTypeName.SET,
        valueType,
        DataTypes.setOf(valueType.cqlType()),
        new ComplexColumnDesc.SetColumnDesc(valueType.columnDesc()));
  }

  public static ApiSetType from(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");

    if (isValueTypeSupported(valueType)) {
      return new ApiSetType((PrimitiveApiDataTypeDef) valueType);
    }
    throw new IllegalArgumentException(
        "valueType must be primitive type, valueType%s".formatted(valueType));
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType.isPrimitive();
  }

  private static final class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiSetType, ComplexColumnDesc.SetColumnDesc> {

    @Override
    public ApiSetType create(
        ComplexColumnDesc.SetColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      ApiDataType valueType;
      try {
        valueType =
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize);
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserType(columnDesc, e);
      }
      // not calling isSupported to avoid double decoding of the valueType
      if (isValueTypeSupported(valueType)) {
        return ApiSetType.from(valueType);
      }
      throw new UnsupportedUserType(columnDesc);
    }

    @Override
    public boolean isSupported(
        ComplexColumnDesc.SetColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      try {
        return isValueTypeSupported(
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }
  }

  private static final class CqlTypeFactory extends TypeFactoryFromCql<ApiSetType, SetType> {

    @Override
    public ApiSetType create(SetType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(cqlType)) {
        throw new UnsupportedCqlType(cqlType);
      }

      try {
        return ApiSetType.from(
            TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn));
      } catch (UnsupportedCqlType e) {
        // make sure we have the set type, not just the key or value type
        throw new UnsupportedCqlType(cqlType, e);
      }
    }

    @Override
    public boolean isSupported(SetType cqlType) {
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
