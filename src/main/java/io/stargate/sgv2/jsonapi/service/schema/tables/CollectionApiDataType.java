package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;

/** Interface defining the api data type for collection types */
public abstract class CollectionApiDataType implements ApiDataType {

  // Default collection support, they cannot be used in filtering
  protected static final ApiSupportDef DEFAULT_API_SUPPORT =
      new ApiSupportDef.Support(true, true, true, false);

  protected final ApiTypeName typeName;
  protected final PrimitiveApiDataTypeDef valueType;
  protected final DataType cqlType;
  protected final ApiSupportDef apiSupport;

  protected CollectionApiDataType(
      ApiTypeName typeName,
      PrimitiveApiDataTypeDef valueType,
      DataType cqlType,
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
}
