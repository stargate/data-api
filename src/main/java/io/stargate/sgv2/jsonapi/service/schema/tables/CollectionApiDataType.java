package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Interface defining the api data type for collection types.
 *
 * @param <T> DataType T is a generic type that can be map/set/list/vector. We are making this
 *     generic because the driver has some properties only available on the leaf level data types.
 */
public abstract class CollectionApiDataType<T extends DataType> implements ApiDataType {

  // Default collection support
  private static final ApiSupportDef DEFAULT_API_SUPPORT =
      new ApiSupportDef.Support(
          true, true, true, true, ApiSupportDef.Update.FULL);

  // Default collection support when the type is frozen, they cannot be used for create, but we can
  // insert them
  private static final ApiSupportDef DEFAULT_API_SUPPORT_FROZEN =
      new ApiSupportDef.Support(
          false, true, true, true, ApiSupportDef.Update.NONE);

  protected static ApiSupportDef defaultApiSupport(boolean isFrozen) {
    return isFrozen ? DEFAULT_API_SUPPORT_FROZEN : DEFAULT_API_SUPPORT;
  }

  protected static final SupportBindingRules SUPPORT_BINDING_RULES =
      new SupportBindingRules(
          SupportBindingRules.create(TypeBindingPoint.COLLECTION_VALUE, false, false),
          SupportBindingRules.create(TypeBindingPoint.MAP_KEY, false, false),
          SupportBindingRules.create(TypeBindingPoint.TABLE_COLUMN, true, true),
          SupportBindingRules.create(TypeBindingPoint.UDT_FIELD, false, false));

  protected final ApiTypeName typeName;
  protected final ApiDataType valueType;
  protected final T cqlType;

  /**
   * We don't support nested collection datatypes
   */
  protected final ApiSupportDef apiSupport;

  protected CollectionApiDataType(
      ApiTypeName typeName, ApiDataType valueType, T cqlType, ApiSupportDef apiSupport) {
    // no null checks here, so subclasses can pass null and then override to create on demand if
    // they want to.
    this.typeName = typeName;
    this.valueType = valueType;
    this.cqlType = cqlType;
    this.apiSupport = apiSupport;
  }

  @Override
  public ApiTypeName typeName() {
    return typeName;
  }

  @Override
  public ApiSupportDef apiSupport() {
    return apiSupport;
  }

  @Override
  public DataType cqlType() {
    return cqlType;
  }

  public ApiDataType getValueType() {
    return valueType;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    var builder = ApiDataType.super.recordTo(dataRecorder);
    return builder.append("valueType", valueType);
  }

  public abstract boolean isFrozen();

}
