package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Optional;

/**
 * Factory for creating a primitive {@link ApiDataType} from a {@link DataType} provided by the
 * driver / cql
 *
 * <p>...
 */
public class PrimitiveTypeFactoryFromCql extends TypeFactoryFromCql<ApiDataType, DataType> {

  // we can always cache the primitive types, so pre-calc the key
  private final Optional<CqlTypeKey> cacheKey;

  private final PrimitiveApiDataTypeDef primitiveTypeInstance;

  public PrimitiveTypeFactoryFromCql(PrimitiveApiDataTypeDef primitiveType) {
    super(primitiveType.cqlType().getProtocolCode(), DataType.class);
    this.primitiveTypeInstance = primitiveType;
    this.cacheKey = Optional.of(CqlTypeKey.create(primitiveType.cqlType(), null, null, false));
  }

  @Override
  public ApiDataType create(
      TypeBindingPoint bindingPoint, DataType cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType {

    if (!isTypeBindable(bindingPoint, cqlType)) {
      throw new UnsupportedCqlType(bindingPoint, cqlType);
    }
    return primitiveTypeInstance;
  }

  @Override
  public boolean isTypeBindable(TypeBindingPoint bindingPoint, DataType cqlType) {
    return primitiveTypeInstance.typeBindingRules().rule(bindingPoint).bindableFromDb();
  }

  @Override
  public Optional<CqlTypeKey> maybeCreateCacheKey(TypeBindingPoint bindingPoint, DataType cqlType) {
    return cacheKey;
  }
}
