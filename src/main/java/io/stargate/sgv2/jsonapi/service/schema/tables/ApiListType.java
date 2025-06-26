package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ListColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiListType extends CollectionApiDataType<ListType> {

  public static final TypeFactoryFromColumnDesc<ApiListType, ListColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiListType, ListType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  public static final ApiSupportDef API_SUPPORT = defaultApiSupport(false);

  private ApiListType(ApiDataType valueType, ApiSupportDef apiSupport, boolean isFrozen) {
    super(ApiTypeName.LIST, valueType, DataTypes.listOf(valueType.cqlType(), isFrozen), apiSupport);
  }

  @Override
  public boolean isFrozen() {
    return cqlType.isFrozen();
  }

  @Override
  public ColumnDesc columnDesc() {
    return new ListColumnDesc(valueType.columnDesc(), ApiSupportDesc.from(this));
  }

  /**
   * Creates a new {@link ApiListType} from the given ApiDataType value.
   *
   * <p>ApiSupport for valueType should be already validated.
   */
  static ApiListType from(ApiDataType valueType, boolean isFrozen) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    if (isValueTypeSupported(valueType)) {
      return new ApiListType(valueType, defaultApiSupport(isFrozen), isFrozen);
    }
    throw new IllegalArgumentException(
        "valueType is not supported, valueType%s".formatted(valueType));
  }

  public static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType.apiSupport().collection().asListValue();
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
        // can never be frozen from the API
        return ApiListType.from(valueType, false);
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

      try {
        ApiDataType valueType =
            TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn);
        if (!isValueTypeSupported(valueType)) {
          throw new UnsupportedCqlType(cqlType);
        }
        return ApiListType.from(
            TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn),
            cqlType.isFrozen());
      } catch (UnsupportedCqlType e) {
        // make sure we have the list type, not just the key or value type
        throw new UnsupportedCqlType(cqlType, e);
      }
    }

    @Override
    public boolean isSupported(ListType cqlType) {
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
