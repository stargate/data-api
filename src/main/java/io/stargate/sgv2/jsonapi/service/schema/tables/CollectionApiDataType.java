package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;

/** Interface defining the api data type for collection types */
public abstract class CollectionApiDataType<T extends DataType> implements ApiDataType {

  // Default collection support, they cannot be used in filtering
  private static final ApiSupportDef DEFAULT_API_SUPPORT =
      new ApiSupportDef.Support(true, true, true, false);

  // Default collection support when the type is frozen, they cannot be used for create but we can
  // insert them
  private static final ApiSupportDef DEFAULT_API_SUPPORT_FROZEN =
      new ApiSupportDef.Support(false, true, true, false);

  protected static ApiSupportDef defaultApiSupport(boolean isFrozen) {
    return isFrozen ? DEFAULT_API_SUPPORT_FROZEN : DEFAULT_API_SUPPORT;
  }

  protected final ApiTypeName typeName;
  protected final PrimitiveApiDataTypeDef valueType;
  protected final T cqlType;
  protected final ApiSupportDef apiSupport;

  protected CollectionApiDataType(
      ApiTypeName typeName,
      PrimitiveApiDataTypeDef valueType,
      T cqlType,
      ApiSupportDef apiSupport) {
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
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public boolean isContainer() {
    return true;
  }

  @Override
  public ApiSupportDef apiSupport() {
    return apiSupport;
  }

  @Override
  public DataType cqlType() {
    return cqlType;
  }

  public PrimitiveApiDataTypeDef getValueType() {
    return valueType;
  }

  public abstract boolean isFrozen();
}
