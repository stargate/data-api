package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for factories that create {@link ApiDataType} from a {@link ColumnDesc} passed it from the
 * user.
 *
 * <p>
 *
 * @param <ApiT>
 * @param <DescT>
 */
public abstract class TypeFactoryFromColumnDesc<ApiT extends ApiDataType, DescT extends ColumnDesc>
    extends FactoryFromDesc {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactoryFromColumnDesc.class);

  private final ApiTypeName apiTypeName;
  private final Class<DescT> descClass;

  protected TypeFactoryFromColumnDesc(ApiTypeName apiTypeName, Class<DescT> descClass) {
    this.apiTypeName = apiTypeName; // allow null for the default factory
    this.descClass = Objects.requireNonNull(descClass, "descClass must not be null");
  }

  /**
   * Called to get the name of the API type that this factory creates, so that decisions can be made
   * based on the type the factory creates.
   */
  public ApiTypeName apiTypeName() {
    return apiTypeName;
  }

  public ApiT createUntyped(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType {

    if (!descClass.isInstance(columnDesc)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromColumnDesc.createUntyped() - columnDesc is not an instance of "
              + descClass.getName());
    }
    return create(bindingPoint, descClass.cast(columnDesc), validateVectorize);
  }

  /**
   * Implementations should create a {@link io.stargate.sgv2.jsonapi.exception.SchemaException} if
   * the usage is not allowed for the given {@link TypeBindingPoint} and {@link ColumnDesc}. Wrapped
   * it in the {@link UnsupportedUserType} exception so that callers can handle it properly.
   */
  public abstract ApiT create(
      TypeBindingPoint bindingPoint, DescT columnDesc, VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType;

  public boolean isSupportedUntyped(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize) {

    if (!descClass.isInstance(columnDesc)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromColumnDesc.isSupportedUntyped() - columnDesc is not an instance of "
              + descClass.getName());
    }
    return isSupported(bindingPoint, descClass.cast(columnDesc), validateVectorize);
  }

  public abstract boolean isSupported(
      TypeBindingPoint bindingPoint, DescT columnDesc, VectorizeConfigValidator validateVectorize);

  public UnsupportedApiDataType createUnsupported(DescT columnDesc) {
    return new UnsupportedUserApiDataType(columnDesc);
  }
}
