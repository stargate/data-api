package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypeFactoryFromCql<ApiT extends ApiDataType, CqlT extends DataType>
    extends FactoryFromCql {

  private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactoryFromCql.class);

  private final Class<CqlT> cqlTypeClass;
  private final ApiTypeName apiTypeName;

  protected TypeFactoryFromCql(ApiTypeName apiTypeName, Class<CqlT> cqlTypeClass) {
    this.cqlTypeClass = cqlTypeClass;
    this.apiTypeName = apiTypeName;
  }

  /**
   * Called to get the name of the API type that this factory creates, so that decisions can be made
   * based on the type the factory creates.
   */
  public ApiTypeName apiTypeName() {
    return apiTypeName;
  }

  public Class<CqlT> cqlTypeClass() {
    return cqlTypeClass;
  }

  public ApiT createUntyped(
      TypeBindingPoint bindingPoint, DataType cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType {
    if (!cqlTypeClass.isInstance(cqlType)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromCql.createUntyped() - cqlType is not an instance of "
              + cqlTypeClass.getName());
    }
    return create(bindingPoint, cqlTypeClass.cast(cqlType), vectorizeDefn);
  }

  public abstract ApiT create(
      TypeBindingPoint bindingPoint, CqlT cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType;

  public boolean isSupportedUntyped(TypeBindingPoint bindingPoint, DataType cqlType) {
    if (!cqlTypeClass.isInstance(cqlType)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromCql.isSupportedUntyped() - cqlType is not an instance of "
              + cqlTypeClass.getName());
    }
    return isSupported(bindingPoint, cqlTypeClass.cast(cqlType));
  }

  /**
   * Called to check if the CQL type from the driver is supported by this factory for the binding
   * point.
   *
   * @param bindingPoint Where the type is being used.
   * @param cqlType The CQL type to check.
   * @return true if the type is supported, false otherwise.
   */
  public abstract boolean isSupported(TypeBindingPoint bindingPoint, CqlT cqlType);

  public UnsupportedApiDataType createUnsupported(CqlT cqlType) {
    return new UnsupportedCqlApiDataType(cqlType);
  }

  public Optional<CqlTypeKey> maybeCreateCacheKeyUntyped(
      TypeBindingPoint bindingPoint, DataType cqlType) {
    if (!cqlTypeClass.isInstance(cqlType)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromCql.maybeCreateCacheKeyUntyped() - cqlType is not an instance of "
              + cqlTypeClass.getName());
    }
    return maybeCreateCacheKey(bindingPoint, cqlTypeClass.cast(cqlType));
  }

  public abstract Optional<CqlTypeKey> maybeCreateCacheKey(
      TypeBindingPoint bindingPoint, CqlT cqlType);
}
