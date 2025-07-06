package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescBindingPoint;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.UdtRefColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.TypeFactoryFromColumnDesc;
import java.util.Objects;

/**
 * A shallow representation of a User Defined Type, it is shallow because it does not include the
 * list of fields, it only includes the name and whether it is frozen or not.
 *
 * <p>This is this the type we make when we have a UDT as a column or collection value type, we use
 * the regular {@link ApiUdtType} when we know the full UDT definition such as when we are creating
 * a table or read the UDT from the driver schema.
 *
 * <p>We make this using the {@link #FROM_COLUMN_DESC_FACTORY}, there is not a factory to create it
 * from CQL.
 */
public class ApiUdtShallowType implements ApiDataType {

  /** Factory to create {@link ApiUdtShallowType} from {@link UdtRefColumnDesc}. */
  public static final TypeFactoryFromColumnDesc<ApiUdtShallowType, UdtRefColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ApiUdtShallowType.ColumnDescFactory();

  protected static final SupportBindingRules UDT_BINDING_RULES =
      new SupportBindingRules(
          SupportBindingRules.create(TypeBindingPoint.COLLECTION_VALUE, true, true),
          SupportBindingRules.create(TypeBindingPoint.MAP_KEY, false, false),
          SupportBindingRules.create(TypeBindingPoint.TABLE_COLUMN, true, true),
          SupportBindingRules.create(TypeBindingPoint.UDT_FIELD, false, false));

  /** Frozen UDTs are used in collection values, and may be created by CQL users. */
  public static final ApiSupportDef API_SUPPORT_FROZEN_UDT =
      new ApiSupportDef.Support(
          false,  true, true, false, ApiSupportDef.Update.UDT);

  /** Normal UDT usage in a column is non-frozen */
  public static final ApiSupportDef API_SUPPORT_NON_FROZEN_UDT =
      new ApiSupportDef.Support(
          true, true, true, false, ApiSupportDef.Update.UDT);

  private final CqlIdentifier udtName;
  private final boolean isFrozen;

  /**
   * NOTE: this is the Driver Shallow type, see {@link ShallowUserDefinedType} created from {@link
   * com.datastax.oss.driver.api.querybuilder.SchemaBuilder#udt(CqlIdentifier, boolean)}
   */
  private final DataType shallowCqlType;

  private final ApiSupportDef apiSupport;

  /** TODO: COMMENTS */
  protected ApiUdtShallowType(CqlIdentifier udtName, boolean isFrozen) {
    this.udtName = udtName;
    this.isFrozen = isFrozen;
    this.shallowCqlType = SchemaBuilder.udt(udtName, isFrozen);
    this.apiSupport = isFrozen ? API_SUPPORT_FROZEN_UDT : API_SUPPORT_NON_FROZEN_UDT;
  }

  /**
   * Throws {@link UnsupportedOperationException} because we should only be using this type on the
   * incoming side of a createTable. Once the table or type is created, we should have the full
   * {@link ApiUdtType}
   */
  @Override
  public ColumnDesc getSchemaDescription(SchemaDescBindingPoint bindingPoint) {
    throw new UnsupportedOperationException(
        "ApiUdtShallowType.getSchemaDescription() is not implemented, use ApiUdtType instead");
  }

  @Override
  public ApiTypeName typeName() {
    return ApiTypeName.UDT;
  }

  @Override
  public DataType cqlType() {
    return shallowCqlType;
  }

  @Override
  public ApiSupportDef apiSupport() {
    return apiSupport;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return ApiDataType.super.recordTo(dataRecorder)
        .append("udtName", udtName)
        .append("isFrozen", isFrozen);
  }

  public CqlIdentifier udtName() {
    return udtName;
  }

  public boolean isFrozen() {
    return isFrozen;
  }

  /**
   * Factory to create {@link ApiUdtShallowType} from user provided {@link UdtRefColumnDesc} as part
   * of creating a table.
   *
   * <p>...
   */
  private static final class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiUdtShallowType, UdtRefColumnDesc> {

    private ColumnDescFactory() {
      super(ApiTypeName.UDT, UdtRefColumnDesc.class);
    }

    @Override
    public ApiUdtShallowType create(
        TypeBindingPoint bindingPoint,
        UdtRefColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      if (!isSupported(bindingPoint, columnDesc, validateVectorize)) {
        // TODO: XXX: AARON need a general schema exception ?
        throw new UnsupportedUserType(bindingPoint, columnDesc, (SchemaException) null);
      }

      if (columnDesc.udtName() == null || columnDesc.udtName().asInternal().isBlank()) {
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.INVALID_USER_DEFINED_TYPE_NAME.get(
                "typeName", errFmt(columnDesc.udtName())));
      }

      return new ApiUdtShallowType(columnDesc.udtName(), columnDesc.isFrozen());
    }

    /** */
    @Override
    public boolean isSupported(
        TypeBindingPoint bindingPoint,
        UdtRefColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      // can we use a udt of any type in this binding point?
      if (!UDT_BINDING_RULES.rule(bindingPoint).supportedFromUser()) {
        return false;
      }

      // NOTE: we cannot check all the fields in the UDT because this is just a reference to a
      // UDT. The UDT def with fields is in {@link ApiUdtType} and is created for the full UDT
      return true;
    }
  }
}
