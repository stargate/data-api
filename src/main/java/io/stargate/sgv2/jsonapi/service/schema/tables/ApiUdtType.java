package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.*;
import java.util.stream.Collectors;

public class ApiUdtType implements ApiDataType {

  /** API type name is always {@link ApiTypeName#UDT}. */
  private final ApiTypeName typeName;

  /** The CqlIdentifier of the UDT, should be validated as not empty and not null. */
  private final CqlIdentifier udtName;

  /**
   * The CQL type of the UDT, which is build as {@link UserDefinedType} in the driver {@link
   * SchemaBuilder}. E.G. <code> SchemaBuilder.udt("address", false) </code>
   */
  private final DataType cqlType;

  /**
   * Indicates if the UDT is frozen or not. As of June 18th 2025, only non-frozen UDT column are
   * supported for table create/alter commands. UDT inside of List/Set/Map are always frozen.
   */
  private final boolean frozen;

  /** The fieldApiTypes is a map of the UDT field name to its ApiDataType. */
  private final Map<CqlIdentifier, ApiDataType> fieldApiTypes;

  /** The apiSupport indicates the API support level for this UDT type. */
  private final ApiSupportDef apiSupport;

  /**
   * Although API does not support create table for frozen UDTs, we still need to read schema from
   * existing CQL tables.
   */
  public static final ApiSupportDef API_SUPPORT_FROZEN_UDT =
      new ApiSupportDef.Support(
          false, ApiSupportDef.Collection.FROZEN_UDT, true, true, false, ApiSupportDef.Update.UDT);

  /** API only supports non-frozen UDTs for create table/alter table commands. */
  public static final ApiSupportDef API_SUPPORT_NON_FROZEN_UDT =
      new ApiSupportDef.Support(
          true, ApiSupportDef.Collection.FROZEN_UDT, true, true, false, ApiSupportDef.Update.UDT);

  /**
   * Factory to create {@link ApiUdtType} from {@link UDTColumnDesc}. This is used in the {@link
   * io.stargate.sgv2.jsonapi.service.schema.tables.TypeFactoryFromColumnDesc} to create the
   * ApiDataType from the columnDesc.
   */
  public static final TypeFactoryFromColumnDesc<ApiUdtType, UDTColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ApiUdtType.ColumnDescFactory();

  /**
   * Factory to create {@link ApiUdtType} from cqlDataType{@link UserDefinedType}. This is used in
   * {@link TypeFactoryFromCql} to create ApiDataType from the cqlDataType
   */
  public static final TypeFactoryFromCql<ApiUdtType, UserDefinedType> FROM_CQL_FACTORY =
      new ApiUdtType.CqlTypeFactory();

  public ApiUdtType(CqlIdentifier udtName, boolean frozen, ApiSupportDef apiSupport) {
    this.typeName = ApiTypeName.UDT;
    this.udtName = udtName;
    this.cqlType = SchemaBuilder.udt(udtName, frozen);
    this.frozen = frozen;
    this.apiSupport = Objects.requireNonNull(apiSupport, "apiSupport must not be null");
    // Note, from the UDTColumnDesc, we do not have the fieldApiTypes, so we set it emtpy list.
    this.fieldApiTypes = Map.of();
  }

  public ApiUdtType(
      CqlIdentifier udtName,
      boolean frozen,
      ApiSupportDef apiSupport,
      Map<CqlIdentifier, ApiDataType> fieldApiTypes) {
    this.typeName = ApiTypeName.UDT;
    this.udtName = udtName;
    this.cqlType = SchemaBuilder.udt(udtName, frozen);
    this.frozen = frozen;
    this.apiSupport = Objects.requireNonNull(apiSupport, "apiSupport must not be null");
    this.fieldApiTypes = Objects.requireNonNull(fieldApiTypes, "fieldApiTypes must not be null");
  }

  /** Returns the {@link ColumnDesc} constructed from the ApiUdtType. */
  @Override
  public ColumnDesc columnDesc() {
    // map each field ApiDataType to its ColumnDesc
    Map<CqlIdentifier, ColumnDesc> fieldDescMap =
        fieldApiTypes.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().columnDesc()));
    return new UDTColumnDesc(udtName, fieldDescMap, frozen);
  }

  private static final class CqlTypeFactory
      extends TypeFactoryFromCql<ApiUdtType, UserDefinedType> {

    @Override
    public ApiUdtType create(UserDefinedType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      Map<CqlIdentifier, ApiDataType> fields = new HashMap<>();
      // There is no map setup in driver to represent the UDT fields name and types.
      // So have to iterate by using indexes for two lists below.
      for (int i = 0; i < cqlType.getFieldNames().size(); i++) {
        var fieldIdentifier = cqlType.getFieldNames().get(i);
        var fieldCqlType = cqlType.getFieldTypes().get(i);
        ApiDataType fieldApiType = TypeFactoryFromCql.DEFAULT.create(fieldCqlType, vectorizeDefn);
        fields.put(fieldIdentifier, fieldApiType);
      }

      var ApiSupportDef = cqlType.isFrozen() ? API_SUPPORT_FROZEN_UDT : API_SUPPORT_NON_FROZEN_UDT;
      return new ApiUdtType(cqlType.getName(), cqlType.isFrozen(), ApiSupportDef, fields);
    }

    @Override
    public boolean isSupported(UserDefinedType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");
      for (DataType fieldType : cqlType.getFieldTypes()) {
        try {
          TypeFactoryFromCql.DEFAULT.create(fieldType, null);
        } catch (UnsupportedCqlType e) {
          return false;
        }
      }
      return true;
    }
  }

  private static final class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiUdtType, UDTColumnDesc> {

    @Override
    public ApiUdtType create(UDTColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      for (Map.Entry<CqlIdentifier, ColumnDesc> fieldColumnDesc :
          columnDesc.fieldsDesc().entrySet()) {
        try {
          TypeFactoryFromColumnDesc.DEFAULT.create(fieldColumnDesc.getValue(), validateVectorize);
        } catch (UnsupportedUserType e) {
          throw new UnsupportedUserType(columnDesc, e);
        }
      }

      var apiSupport = columnDesc.isFrozen() ? API_SUPPORT_FROZEN_UDT : API_SUPPORT_NON_FROZEN_UDT;
      return new ApiUdtType(columnDesc.udtName, columnDesc.isFrozen(), apiSupport);
    }

    /** */
    @Override
    public boolean isSupported(
        UDTColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      for (Map.Entry<CqlIdentifier, ColumnDesc> fieldColumnDesc :
          columnDesc.fieldsDesc().entrySet()) {
        try {
          TypeFactoryFromColumnDesc.DEFAULT.create(fieldColumnDesc.getValue(), validateVectorize);
        } catch (UnsupportedUserType e) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public ApiTypeName typeName() {
    return typeName;
  }

  @Override
  public DataType cqlType() {
    return cqlType;
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public boolean isContainer() {
    return false;
  }

  @Override
  public ApiSupportDef apiSupport() {
    return apiSupport;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return Objects.equals(typeName, ((ApiUdtType) o).typeName)
        && Objects.equals(cqlType, ((ApiUdtType) o).cqlType)
        && Objects.equals(udtName, ((ApiUdtType) o).udtName)
        && Objects.equals(frozen, ((ApiUdtType) o).frozen)
        && Objects.equals(fieldApiTypes, ((ApiUdtType) o).fieldApiTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeName, cqlType, udtName, frozen, fieldApiTypes);
  }

  @Override
  public String toString() {
    return "ApiUdtType{"
        + "typeName="
        + typeName
        + ", cqlType="
        + cqlType
        + ", udtName="
        + udtName
        + ", frozen="
        + frozen
        + ", fieldApiTypes="
        + fieldApiTypes
        + ", apiSupport="
        + apiSupport
        + '}';
  }
}
