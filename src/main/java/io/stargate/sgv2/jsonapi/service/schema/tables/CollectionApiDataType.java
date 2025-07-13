package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Interface defining the api data type for collection types.
 *
 * @param <T> DataType T is a generic type that can be map/set/list/vector. We are making this
 *     generic because the driver has some properties only available on the leaf level data types.
 */
public abstract class CollectionApiDataType<T extends DataType> implements ApiDataType {

  /**
   * When collection is not frozen we do all operations. See {@link
   * #defaultCollectionApiSupport(boolean)}
   */
  private static final ApiSupportDef DEFAULT_API_SUPPORT =
      new ApiSupportDef.Support(true, true, true, true, ApiSupportDef.Update.FULL);

  /**
   * When the collection is frozen, we cannot use it for create, or mutating the value of the
   * column, but we can insert, read, and do $set and $unset on the whole value. See {@link
   * #defaultCollectionApiSupport(boolean)}
   */
  private static final ApiSupportDef DEFAULT_API_SUPPORT_FROZEN =
      new ApiSupportDef.Support(
          false, true, true, true, new ApiSupportDef.Update(true, true, false, false));

  protected static final DefaultTypeBindingRules SUPPORT_BINDING_RULES =
      new DefaultTypeBindingRules(
          DefaultTypeBindingRules.create(TypeBindingPoint.COLLECTION_VALUE, false, false),
          DefaultTypeBindingRules.create(TypeBindingPoint.MAP_KEY, false, false),
          DefaultTypeBindingRules.create(TypeBindingPoint.TABLE_COLUMN, true, true),
          DefaultTypeBindingRules.create(TypeBindingPoint.UDT_FIELD, false, false));

  protected final ApiTypeName typeName;
  protected final ApiDataType valueType;
  protected final T cqlType;

  /** We don't support nested collection datatypes */
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

  public abstract boolean isFrozen();

  /** Gets the collection default API support based on whether the collection is frozen or not. */
  protected static ApiSupportDef defaultCollectionApiSupport(boolean isFrozen) {
    return isFrozen ? DEFAULT_API_SUPPORT_FROZEN : DEFAULT_API_SUPPORT;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    var builder = ApiDataType.super.recordTo(dataRecorder);
    return builder.append("valueType", valueType);
  }
}
