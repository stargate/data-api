package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.SetType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.SetColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiSetType extends CollectionApiDataType<SetType> {

  public static final TypeFactoryFromColumnDesc<ApiSetType, SetColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiSetType, SetType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  public static final ApiSupportDef API_SUPPORT = defaultApiSupport(false);

  private ApiSetType(
      PrimitiveApiDataTypeDef valueType, ApiSupportDef apiSupport, boolean isFrozen) {
    super(ApiTypeName.SET, valueType, DataTypes.setOf(valueType.cqlType(), isFrozen), apiSupport);
  }

  @Override
  public boolean isFrozen() {
    return cqlType.isFrozen();
  }

  @Override
  public ColumnDesc columnDesc() {
    return new SetColumnDesc(valueType.columnDesc(), ApiSupportDesc.from(this));
  }

  /**
   * Creates a new {@link ApiSetType} from the given ApiDataType value.
   *
   * <p>ApiSupport for valueType should be already validated.
   */
  static ApiSetType from(ApiDataType valueType, boolean isFrozen) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    if (isValueTypeSupported(valueType)) {
      return new ApiSetType(
          (PrimitiveApiDataTypeDef) valueType, defaultApiSupport(isFrozen), isFrozen);
    }
    throw new IllegalArgumentException(
        "valueType is not supported, valueType%s".formatted(valueType));
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType.apiSupport().collection().asSetValue();
  }

  private static final class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiSetType, SetColumnDesc> {

    @Override
    public ApiSetType create(SetColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
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
        // never frozen from the API
        return ApiSetType.from(valueType, false);
      }
      throw new UnsupportedUserType(columnDesc);
    }

    @Override
    public boolean isSupported(
        SetColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
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

      try {
        ApiDataType valueType =
            TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn);
        if (!isValueTypeSupported(valueType)) {
          throw new UnsupportedCqlType(cqlType);
        }
        return ApiSetType.from(
            TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn),
            cqlType.isFrozen());
      } catch (UnsupportedCqlType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedCqlType(cqlType, e);
      }
    }

    @Override
    public boolean isSupported(SetType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      //  we accept frozen, but change the support.
      try {
        var valueType = TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), null);
        return isValueTypeSupported(valueType);
      } catch (UnsupportedCqlType e) {
        return false;
      }
    }
  }
}
