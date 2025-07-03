package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Factory for creating a primitive {@link ApiDataType} from a {@link DataType} provided by the
 * driver / cql
 *
 * <p>...
 */
public class PrimitiveTypeFactoryFromCql extends TypeFactoryFromCql<ApiDataType, DataType> {

  // we can always cache the primitive types, so pre-calc the key
  private final Optional<CqlTypeKey> cacheKey;

  public static final Map<DataType, PrimitiveTypeFactoryFromCql> ALL_FACTORIES;

  static {
    ALL_FACTORIES =
        Collections.unmodifiableMap(
            PRIMITIVE_TYPES.stream()
                .collect(
                    Collectors.toMap(
                        PrimitiveApiDataTypeDef::cqlType, PrimitiveTypeFactoryFromCql::new)));
  }

  private PrimitiveApiDataTypeDef primitiveTypeInstance;

  private PrimitiveTypeFactoryFromCql(PrimitiveApiDataTypeDef primitiveType) {
    super(primitiveType.typeName(), DataType.class);
    this.primitiveTypeInstance = primitiveType;
    this.cacheKey = Optional.of(CqlTypeKey.create(primitiveType.cqlType(), null, null, false));
  }

  @Override
  public ApiTypeName apiTypeName() {
    return primitiveTypeInstance.typeName();
  }

  @Override
  public ApiDataType create(
      TypeBindingPoint bindingPoint, DataType cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType {

    return primitiveTypeInstance;
  }

  @Override
  public boolean isSupported(TypeBindingPoint bindingPoint, DataType cqlType) {
    // Primitive types are always supported in all situations
    return true;
  }

  @Override
  public Optional<CqlTypeKey> maybeCreateCacheKey(TypeBindingPoint bindingPoint, DataType cqlType) {
    return cacheKey;
  }
}
