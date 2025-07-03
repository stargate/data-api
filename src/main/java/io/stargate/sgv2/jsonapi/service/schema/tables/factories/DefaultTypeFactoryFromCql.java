package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.*;
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

  private static final Map<Class<?>, TypeFactoryFromCql<? extends ApiDataType, ? extends DataType>>
      ALL_FACTORIES;

  static {
    Map<Class<?>, TypeFactoryFromCql<? extends ApiDataType, ? extends DataType>> factories =
        new HashMap<>();

    addFactory(factories, ApiVectorType.FROM_CQL_FACTORY);
    addFactory(factories, ApiUdtType.FROM_CQL_FACTORY);
    addFactory(factories, ApiMapType.FROM_CQL_FACTORY);
    addFactory(factories, ApiListType.FROM_CQL_FACTORY);
    addFactory(factories, ApiSetType.FROM_CQL_FACTORY);

    PrimitiveTypeFactoryFromCql.ALL_FACTORIES
        .values()
        .forEach(factory -> addFactory(factories, factory));

    ALL_FACTORIES = Collections.unmodifiableMap(factories);
  }

  /**
   * Cache of types created when the factory returned a non-empty value from {@link
   * TypeFactoryFromCql#maybeCreateCacheKey(TypeBindingPoint, DataType)}
   */
  private static final ConcurrentMap<CqlTypeKey, ApiDataType> TYPE_CACHE =
      new ConcurrentHashMap<>();

  public DefaultTypeFactoryFromCql() {
    super(null, DataType.class);
  }

  public static <CqlT extends DataType>
      TypeFactoryFromCql<? extends ApiDataType, ? extends DataType> factoryFor(
          TypeBindingPoint bindingPoint, CqlT cqlType) throws UnsupportedCqlType {

    var factory = ALL_FACTORIES.get(cqlType.getClass());
    if (factory != null) {
      return factory;
    }

    throw new UnsupportedCqlType(bindingPoint, cqlType);
  }

  private static void addFactory(
      Map<Class<?>, TypeFactoryFromCql<? extends ApiDataType, ? extends DataType>> map,
      TypeFactoryFromCql<? extends ApiDataType, ? extends DataType> factory) {
    map.put(factory.cqlTypeClass(), factory);
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
              // This can happen if it there are parts of the type we do not support,
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

    //    // ++++++++++
    //
    //    // primitive types are basic, they singleton and cached by their factory
    //    if (wildcardFactory.apiTypeName().isPrimitive()) {
    //      return wildcardFactory.createUntyped(bindingPoint, cqlType, vectorizeDefn);
    //    }
    //
    //    // Do not cache the Vector type because the ApiVectorType has the user defined
    // vectorization
    //    // on it, and they have unique config for the length.
    //    // Note vector is flagged as container type, so we need to check and handle first
    //    if (wildcardFactory.apiTypeName() == ApiTypeName.VECTOR) {
    //      return wildcardFactory.createUntyped(bindingPoint, cqlType, vectorizeDefn);
    //    }
    //
    //    // Do not cache the UDT type because the UDT schema are unique per keyspace and per
    // tenant.
    //    if (wildcardFactory.apiTypeName() == ApiTypeName.UDT) {
    //      return ApiUdtType.FROM_CQL_FACTORY.createUntyped(bindingPoint, cqlType, vectorizeDefn);
    //    }
    //
    //    // Here it should be a container type, sanity check
    //    if (!wildcardFactory.apiTypeName().isContainer()){
    //      throw new IllegalStateException("DefaultTypeFactoryFromCql.create() - expected only
    // container types, got: "
    //          + wildcardFactory.apiTypeName());
    //    }
    //
    //    // See CollectionCacheKey for why we need it
    //    // if we cannot get a cache key we do not support the type
    //    // if we can, it should be something covered in computeIfAbsent()
    //    var cacheKey =
    //        CollectionCacheKey.maybeCreate(cqlType)
    //            .orElseThrow(() -> new UnsupportedCqlType(cqlType));
    //
    //    try {
    //      return COLLECTION_TYPE_CACHE.computeIfAbsent(
    //          cacheKey,
    //          entry -> {
    //            try {
    //              // Sanity check that the factory we got at the start is for the type the key
    // wants.
    //              if (entry.apiTypeName != wildcardFactory.apiTypeName()) {
    //                throw new IllegalStateException(
    //                    "DefaultTypeFactoryFromCql.create() - cache entry.apiTypeName():  "
    //                        + entry.apiTypeName
    //                        + " but wildcardFactory.apiTypeName(): "
    //                        + wildcardFactory.apiTypeName());
    //              }
    //              // we know this will be a collection type, so we can cast it safely
    //              return (CollectionApiDataType<?>) wildcardFactory.createUntyped(
    //                  bindingPoint, entry.cqlType, vectorizeDefn);
    //
    //            } catch (UnsupportedCqlType e) {
    //              // This can happen if it is a collection config we do not support, wrap the
    // checked
    //              // exception to get out of the cache loader. See below.
    //              throw new RuntimeException(e);
    //            }
    //          });
    //    } catch (RuntimeException e) {
    //      if (e.getCause() instanceof UnsupportedCqlType uct) {
    //        throw uct;
    //      }
    //      throw e;
    //    }
  }

  @Override
  public boolean isSupported(TypeBindingPoint bindingPoint, DataType cqlType) {

    // throws if we cannot find a factory for the cqlType
    try {
      return factoryFor(bindingPoint, cqlType).isSupportedUntyped(bindingPoint, cqlType);
    } catch (UnsupportedCqlType e) {
      // if we could not find a factory for the cqlType, then it is not supported
      return false;
    }
  }

  @Override
  public ApiTypeName apiTypeName() {
    throw new UnsupportedOperationException(
        "DefaultTypeFactoryFromCql.apiTypeName() is not implemented");
  }

  @Override
  public Optional<CqlTypeKey> maybeCreateCacheKey(TypeBindingPoint bindingPoint, DataType cqlType) {
    throw new UnsupportedOperationException(
        "DefaultTypeFactoryFromCql.maybeCreateCacheKey() is not implemented");
  }

  /**
   * The equals for collection data types in the driver does not take frozen into account. This is a
   * problem for the API because we do not support frozen types. So if we only used the DataType
   * from the driver the cache key ofr a frozen <code>map<string, sting></code> would be the same
   * for a non-frozen version.
   *
   * <p>Using the protocol codes because that is the simplest way to detect types, there are no
   * values below 0 see {@link com.datastax.oss.protocol.internal.ProtocolConstants.DataType}
   */
  //  private static class CollectionCacheKey {
  //
  //    private final int collectionProtoCode;
  //    private final int keyProtoCode;
  //    private final int valueProtoCode;
  //    private final boolean isFrozen;
  //
  //    // visible for the computeIfAbsent function to get
  //    final DataType cqlType;
  //
  //    // visible for the computeIfAbsent function to get
  //    final ApiTypeName apiTypeName;
  //
  //    private CollectionCacheKey(
  //        int collectionProtoCode,
  //        int keyProtoCode,
  //        int valueProtoCode,
  //        boolean isFrozen,
  //        DataType cqlType,
  //        ApiTypeName apiTypeName) {
  //      this.collectionProtoCode = collectionProtoCode;
  //      this.keyProtoCode = keyProtoCode;
  //      this.valueProtoCode = valueProtoCode;
  //      this.isFrozen = isFrozen;
  //      this.cqlType = cqlType;
  //      this.apiTypeName = apiTypeName;
  //    }
  //
  //    static Optional<CollectionCacheKey> maybeCreate(DataType cqlType) {
  //      return switch (cqlType) {
  //        case MapType mt ->
  //            Optional.of(
  //                new CollectionCacheKey(
  //                    mt.getProtocolCode(),
  //                    mt.getKeyType().getProtocolCode(),
  //                    mt.getValueType().getProtocolCode(),
  //                    mt.isFrozen(),
  //                    cqlType,
  //                    ApiTypeName.MAP));
  //        case ListType lt ->
  //            Optional.of(
  //                new CollectionCacheKey(
  //                    lt.getProtocolCode(),
  //                    -1,
  //                    lt.getElementType().getProtocolCode(),
  //                    lt.isFrozen(),
  //                    cqlType,
  //                    ApiTypeName.LIST));
  //        case SetType st ->
  //            Optional.of(
  //                new CollectionCacheKey(
  //                    st.getProtocolCode(),
  //                    -1,
  //                    st.getElementType().getProtocolCode(),
  //                    st.isFrozen(),
  //                    cqlType,
  //                    ApiTypeName.MAP));
  //        default -> {
  //          if (LOGGER.isTraceEnabled()) {
  //            LOGGER.trace(
  //                "CollectionCacheKey does not support supplied CQL type: {}",
  //                cqlType.asCql(true, true));
  //          }
  //          yield Optional.empty();
  //        }
  //      };
  //    }
  //
  //    /**
  //     * We only need to check the protocol codes and frozen status, cqltype is only there for the
  //     * computeIfAbsent()
  //     */
  //    @Override
  //    public boolean equals(Object obj) {
  //      if (this == obj) {
  //        return true;
  //      }
  //      if (obj == null || getClass() != obj.getClass()) {
  //        return false;
  //      }
  //      CollectionCacheKey that = (CollectionCacheKey) obj;
  //      return collectionProtoCode == that.collectionProtoCode
  //          && keyProtoCode == that.keyProtoCode
  //          && valueProtoCode == that.valueProtoCode
  //          && isFrozen == that.isFrozen;
  //    }
  //
  //    /**
  //     * We only need to check the protocol codes and frozen status, cqltype is only there for the
  //     * computeIfAbsent()
  //     */
  //    @Override
  //    public int hashCode() {
  //      return Objects.hash(collectionProtoCode, keyProtoCode, valueProtoCode, isFrozen);
  //    }
  //  }
}
