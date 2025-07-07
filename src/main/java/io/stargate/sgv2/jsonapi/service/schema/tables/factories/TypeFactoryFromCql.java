package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypeFactoryFromCql<ApiT extends ApiDataType, CqlT extends DataType>
    extends FactoryFromCql {

  private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactoryFromCql.class);

  private final int cqlProtocolCode;
  private final Class<CqlT> cqlGenericsClass;

  /**
   * @param cqlProtocolCode {@link DataType#getProtocolCode()} protocol code of the CQL type to
   *     match this factory to. We need to use protocol code because all CQL primitive types have
   *     the same class.
   * @param cqlGenericsClass Class of the generic type, used to check casting by the functions like
   *     {@link #createUntyped(TypeBindingPoint, DataType, VectorizeDefinition)}
   */
  protected TypeFactoryFromCql(int cqlProtocolCode, Class<CqlT> cqlGenericsClass) {
    this.cqlProtocolCode = cqlProtocolCode;
    this.cqlGenericsClass =
        Objects.requireNonNull(cqlGenericsClass, "cqlGenericsClass must not be null");
  }

  protected int cqlProtocolCode() {
    return cqlProtocolCode;
  }

  public ApiT createUntyped(
      TypeBindingPoint bindingPoint, DataType cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType {
    if (!cqlGenericsClass.isInstance(cqlType)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromCql.createUntyped() - cqlType is not an instance of "
              + cqlGenericsClass.getName());
    }
    return create(bindingPoint, cqlGenericsClass.cast(cqlType), vectorizeDefn);
  }

  public abstract ApiT create(
      TypeBindingPoint bindingPoint, CqlT cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType;

  public boolean isTypeBindableUntyped(TypeBindingPoint bindingPoint, DataType cqlType) {
    if (!cqlGenericsClass.isInstance(cqlType)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromCql.isSupportedUntyped() - cqlType is not an instance of "
              + cqlGenericsClass.getName());
    }
    return isTypeBindable(bindingPoint, cqlGenericsClass.cast(cqlType));
  }

  /**
   * Called to check if the CQL type from the driver is supported by this factory for the binding
   * point.
   *
   * @param bindingPoint Where the type is being used.
   * @param cqlType The CQL type to check.
   * @return true if the type is supported, false otherwise.
   */
  public abstract boolean isTypeBindable(TypeBindingPoint bindingPoint, CqlT cqlType);

  public UnsupportedApiDataType createUnsupported(CqlT cqlType) {
    return new UnsupportedCqlApiDataType(cqlType);
  }

  public Optional<CqlTypeKey> maybeCreateCacheKeyUntyped(
      TypeBindingPoint bindingPoint, DataType cqlType) {
    if (!cqlGenericsClass.isInstance(cqlType)) {
      throw new IllegalArgumentException(
          "TypeFactoryFromCql.maybeCreateCacheKeyUntyped() - cqlType is not an instance of "
              + cqlGenericsClass.getName());
    }
    return maybeCreateCacheKey(bindingPoint, cqlGenericsClass.cast(cqlType));
  }

  public abstract Optional<CqlTypeKey> maybeCreateCacheKey(
      TypeBindingPoint bindingPoint, CqlT cqlType);
}
