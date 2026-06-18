package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.Optional;

/**
 * A type factory that only creates the {@link UnsupportedCqlApiDataType} instances from CQL driver
 * instances.
 *
 * <p>We need to have a type factory for every type the database can return, so that we know the
 * different a type we have not coded for and a type we know about but do not support in the API.
 * The former could be a bug in how we map types. Create an instance mapped to the
 */
class UnsupportedTypeFactoryFromCql<T extends DataType>
    extends TypeFactoryFromCql<UnsupportedCqlApiDataType, T> {

  UnsupportedTypeFactoryFromCql(int cqlProtocolCode, Class<T> cqlTypeClass) {
    super(cqlProtocolCode, cqlTypeClass);
  }

  @Override
  public UnsupportedCqlApiDataType create(
      TypeBindingPoint bindingPoint, T cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType {
    // throw so that when the factory is used the caller will then call {@link
    // #createUnsupportedCqlApiDataType}
    throw new UnsupportedCqlType(bindingPoint, cqlType);
  }

  @Override
  public boolean isTypeBindable(TypeBindingPoint bindingPoint, T cqlType) {
    // never supported :)
    return false;
  }

  @Override
  public Optional<CqlTypeKey> maybeCreateCacheKey(TypeBindingPoint bindingPoint, T cqlType) {
    return Optional.empty();
  }
}
