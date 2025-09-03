package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;
import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Factory that can be called to create any {@link ApiDataType}, use via the {@link
 * #INSTANCE}
 *
 * <p>...
 */
public class DefaultTypeFactoryFromCql extends TypeFactoryFromCql<ApiDataType, DataType> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTypeFactoryFromCql.class);

  public static final DefaultTypeFactoryFromCql INSTANCE = new DefaultTypeFactoryFromCql();

  private static final Map<Integer, TypeFactoryFromCql<? extends ApiDataType, ? extends DataType>>
      ALL_FACTORIES;

  static {
    Map<Integer, TypeFactoryFromCql<? extends ApiDataType, ? extends DataType>> factories =
        new HashMap<>();

    addFactory(factories, ApiVectorType.FROM_CQL_FACTORY);
    addFactory(factories, ApiUdtType.FROM_CQL_FACTORY);
    addFactory(factories, ApiMapType.FROM_CQL_FACTORY);
    addFactory(factories, ApiListType.FROM_CQL_FACTORY);
    addFactory(factories, ApiSetType.FROM_CQL_FACTORY);

    PRIMITIVE_TYPES.forEach(
        primitiveType -> addFactory(factories, new PrimitiveTypeFactoryFromCql(primitiveType)));

    // types that we know about, but do not support in any configuration,
    // we need a factory for every type the DB can return,
    // so we can verify that the mapping to find factories is valid.
    factories.put(
        ProtocolConstants.DataType.TUPLE,
        new UnsupportedTypeFactoryFromCql<>(ProtocolConstants.DataType.TUPLE, TupleType.class));
    ALL_FACTORIES = Collections.unmodifiableMap(factories);
  }

  /**
   * Cache of types created when the factory returned a non-empty value from {@link
   * TypeFactoryFromCql#maybeCreateCacheKey(TypeBindingPoint, DataType)}
   */
  private static final ConcurrentMap<CqlTypeKey, ApiDataType> TYPE_CACHE =
      new ConcurrentHashMap<>();

  public DefaultTypeFactoryFromCql() {
    // protocol code will be ignored, this is not indexed by protocol code
    super(-1, DataType.class);
  }

  public static <CqlT extends DataType>
      TypeFactoryFromCql<? extends ApiDataType, ? extends DataType> factoryFor(
          TypeBindingPoint bindingPoint, CqlT cqlType) {

    var factory = ALL_FACTORIES.get(cqlType.getProtocolCode());
    if (factory != null) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "DefaultTypeFactoryFromCql.factoryFor() - found factory bindingPoint: {}, cqlType.asCql: {}, cqlType.getProtocolCode: {}, factory.class: {}",
            bindingPoint,
            cqlType.asCql(true, true),
            cqlType.getProtocolCode(),
            classSimpleName(factory.getClass()));
      }
      return factory;
    }

    // this really should not happen, we should catch this in testing, it means we do not know about
    // the type so throw an error that will cause the API to return a 500 error, rather than a
    // UnsupportedCqlType which can also be thrown by the factory if the configuration of the type
    // is not supported.
    throw new IllegalStateException(
        "DefaultTypeFactoryFromCql.factoryFor() - no factory for cqlType.asCql: %s, cqlType.getProtocolCode: %s"
            .formatted(cqlType.asCql(true, true), cqlType.getProtocolCode()));
  }

  private static void addFactory(
      Map<Integer, TypeFactoryFromCql<? extends ApiDataType, ? extends DataType>> map,
      TypeFactoryFromCql<? extends ApiDataType, ? extends DataType> factory) {

    var existing = map.put(factory.cqlProtocolCode(), factory);
    if (existing != null) {
      throw new IllegalStateException(
          "DefaultTypeFactoryFromCql.addFactory() - existing factory factory.cqlProtocolCode: %s"
              .formatted(factory.cqlProtocolCode()));
    }
  }

  @Override
  public ApiDataType create(
      TypeBindingPoint bindingPoint, DataType cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType {
    Objects.requireNonNull(cqlType, "cqlType must not be null");

    // throws UnsupportedCqlType if we cannot find a factory for the cqlType
    // we could still not support the type as we try to build it, that will be thrown
    // by the factory itself.
    var typeFactory = factoryFor(bindingPoint, cqlType);

    var maybeCacheKey = typeFactory.maybeCreateCacheKeyUntyped(bindingPoint, cqlType);

    // if we did not get a cache key then we cannot cache the type, so we get the
    // factory to create the type directly
    if (maybeCacheKey.isEmpty()) {
      return typeFactory.createUntyped(bindingPoint, cqlType, vectorizeDefn);
    }

    // check the cache, and then call the factory if we need to.
    try {
      return TYPE_CACHE.computeIfAbsent(
          maybeCacheKey.get(),
          entry -> {
            try {
              return typeFactory.createUntyped(bindingPoint, cqlType, vectorizeDefn);
            } catch (UnsupportedCqlType e) {
              // This can happen if there are parts of the type we do not support,
              // like a collection of collection, wrap the checked
              // exception to get it out of the cache loader, then re-throw below
              throw new RuntimeException(e);
            }
          });
    } catch (RuntimeException e) {
      if (e.getCause() instanceof UnsupportedCqlType uct) {
        throw uct;
      }
      throw e;
    }
  }

  @Override
  public boolean isTypeBindable(TypeBindingPoint bindingPoint, DataType cqlType) {

    // throws if we cannot find a factory for the cqlType
    return factoryFor(bindingPoint, cqlType).isTypeBindableUntyped(bindingPoint, cqlType);
  }

  @Override
  public Optional<CqlTypeKey> maybeCreateCacheKey(TypeBindingPoint bindingPoint, DataType cqlType) {
    throw new UnsupportedOperationException(
        "DefaultTypeFactoryFromCql.maybeCreateCacheKey() is not implemented");
  }
}
