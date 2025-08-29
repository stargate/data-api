package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
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
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  protected static final BindingPointRules<UdtBindingPointRule> UDT_BINDING_RULES =
      new BindingPointRules<>(
          new UdtBindingPointRule(TypeBindingPoint.COLLECTION_VALUE, true, true, true),
          new UdtBindingPointRule(TypeBindingPoint.MAP_KEY, false, false, false),
          new UdtBindingPointRule(TypeBindingPoint.TABLE_COLUMN, true, true, false),
          new UdtBindingPointRule(TypeBindingPoint.UDT_FIELD, false, false, false));

  /**
   * Frozen UDTs are used in collection values, and may be created by CQL users. When frozen the
   * fields in the UDT cannot be updated. But you can set and unset the column itself. HACK: for
   * now, we cannot update individual fields in the UDT, so marking $set and $unset is OK, but we
   * have no way to update the fields in the UDT. When support updating fields, we need an update
   * state for setting fields in the UDT, not the columns itself.
   */
  public static final ApiSupportDef API_SUPPORT_FROZEN_UDT =
      new ApiSupportDef.Support(
          true, true, true, true, new ApiSupportDef.Update(true, true, false, false));

  /** Normal UDT usage in a column is non-frozen, we can update it but cannot filter on it */
  public static final ApiSupportDef API_SUPPORT_NON_FROZEN_UDT =
      new ApiSupportDef.Support(true, true, true, false, ApiSupportDef.Update.UDT);

  private final CqlIdentifier udtName;
  private final boolean isFrozen;

  /**
   * NOTE: this is the Driver Shallow type, see {@link ShallowUserDefinedType} created from {@link
   * com.datastax.oss.driver.api.querybuilder.SchemaBuilder#udt(CqlIdentifier, boolean)}
   */
  private final DataType shallowCqlType;

  private final ApiSupportDef apiSupport;

  /**
   * Constructor for creating a shallow UDT type, this is used when we have a UDT as a column or
   * collection value type, but we do not have the full definition of the UDT.
   *
   * @param udtName The name of the UDT.
   * @param isFrozen Whether the UDT is frozen or not.
   */
  protected ApiUdtShallowType(CqlIdentifier udtName, boolean isFrozen) {
    this.udtName = udtName;
    this.isFrozen = isFrozen;
    this.shallowCqlType = SchemaBuilder.udt(udtName, isFrozen);
    this.apiSupport = isFrozen ? API_SUPPORT_FROZEN_UDT : API_SUPPORT_NON_FROZEN_UDT;
  }

  /**
   * Throws {@link UnsupportedOperationException} because we should only be using this shallow type
   * on the incoming side of a createTable. Once the table or type is created, we should have the
   * full {@link ApiUdtType}
   */
  @Override
  public ColumnDesc getSchemaDescription(SchemaDescSource schemaDescSource) {
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
    return ApiDataType.super
        .recordTo(dataRecorder)
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

      if (!isTypeBindable(bindingPoint, columnDesc, validateVectorize)) {
        // currently do not have anything more specific to throw
        throw new UnsupportedUserType(bindingPoint, columnDesc);
      }

      if (columnDesc.udtName() == null || columnDesc.udtName().asInternal().isBlank()) {
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.INVALID_USER_DEFINED_TYPE_NAME.get(
                "typeName", errFmt(columnDesc.udtName())));
      }

      return new ApiUdtShallowType(
          columnDesc.udtName(), UDT_BINDING_RULES.rule(bindingPoint).useFrozenUdt);
    }

    /** */
    @Override
    public boolean isTypeBindable(
        TypeBindingPoint bindingPoint,
        UdtRefColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      // can we use a udt of any type in this binding point?
      if (!UDT_BINDING_RULES.rule(bindingPoint).bindableFromUser()) {
        return false;
      }

      // NOTE: we cannot check all the fields in the UDT because this is just a reference to a
      // UDT. The UDT def with fields is in {@link ApiUdtType} and is created for the full UDT
      return true;
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint) {
      return UDT_BINDING_RULES.rule(bindingPoint).bindableFromUser();
    }
  }

  public record UdtBindingPointRule(
      TypeBindingPoint bindingPoint,
      boolean bindableFromDb,
      boolean bindableFromUser,
      boolean useFrozenUdt)
      implements BindingPointRules.BindingPointRule {}
}
