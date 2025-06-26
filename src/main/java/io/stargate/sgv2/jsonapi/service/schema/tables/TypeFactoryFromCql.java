package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypeFactoryFromCql<ApiT extends ApiDataType, CqlT extends DataType>
    extends FactoryFromCql {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactoryFromCql.class);

  public static final TypeFactoryFromCql<ApiDataType, DataType> DEFAULT = new DefaultFactory();

  public abstract ApiT create(CqlT cqlType, VectorizeDefinition vectorizeDefn)
      throws UnsupportedCqlType;

  public abstract boolean isSupported(CqlT cqlType);

  public UnsupportedApiDataType createUnsupported(CqlT cqlType) {
    return new UnsupportedCqlApiDataType(cqlType);
  }

  private static class DefaultFactory extends TypeFactoryFromCql<ApiDataType, DataType> {

    private static final Map<DataType, PrimitiveApiDataTypeDef> PRIMITIVE_TYPES_BY_CQL_TYPE =
        PRIMITIVE_TYPES.stream()
            .collect(Collectors.toMap(PrimitiveApiDataTypeDef::cqlType, Function.identity()));

    private static final ConcurrentMap<CollectionCacheKey, CollectionApiDataType>
        COLLECTION_TYPE_CACHE = new ConcurrentHashMap<>();

    @Override
    public ApiDataType create(DataType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");
      var primitiveType = PRIMITIVE_TYPES_BY_CQL_TYPE.get(cqlType);
      if (primitiveType != null) {
        return primitiveType;
      }

      // Do not cache the Vector type because the ApiVectorType has the user defined vectorizarion
      // on it
      if (cqlType instanceof VectorType vt) {
        return ApiVectorType.FROM_CQL_FACTORY.create(vt, vectorizeDefn);
      }

      // Do not cache the UDT type because the UDT schema are unique per keyspace and per tenant.
      // Data API should not infer the UDT schema from a simple type cache.
      if (cqlType instanceof UserDefinedType udt) {
        return ApiUdtType.FROM_CQL_FACTORY.create(udt, vectorizeDefn);
      }

      // See CollectionCacheKey for why we need it
      // if we cannot get a cache key we do not support the type
      // if we can, it should be something covered in computeIfAbsent()
      var cacheKey =
          CollectionCacheKey.maybeCreate(cqlType)
              .orElseThrow(() -> new UnsupportedCqlType(cqlType));

      try {
        return COLLECTION_TYPE_CACHE.computeIfAbsent(
            cacheKey,
            entry -> {
              try {
                return switch (entry.cqlType) {
                  case MapType mt -> ApiMapType.FROM_CQL_FACTORY.create(mt, vectorizeDefn);
                  case ListType lt -> ApiListType.FROM_CQL_FACTORY.create(lt, vectorizeDefn);
                  case SetType st -> ApiSetType.FROM_CQL_FACTORY.create(st, vectorizeDefn);
                  default ->
                      throw new IllegalStateException(
                          "No factory for the supplied CQL type: " + cqlType.asCql(true, true));
                };
              } catch (UnsupportedCqlType e) {
                // This can happen if it is a collection config we do not support, wrap the checked
                // exception to get
                // out of the cache loader.
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
    public boolean isSupported(DataType cqlType) {
      return false;
    }
  }

  /**
   * The equals for collection data types in the driver does not take frozen into account. This is a
   * problem for the API because we do not support frozen types. So if we only used the DataType
   * from the driver the cache key ofr a frozen <code>map<string, sting></code> would be the same
   * for a non-frozen version.
   *
   * <p>Using the protocol codes because that is the simpliest way to detect types, there are no
   * values below 0 see {@link com.datastax.oss.protocol.internal.ProtocolConstants.DataType}
   */
  private static class CollectionCacheKey {

    private int collectionProtoCode;
    private int keyProtoCode;
    private int valueProtoCode;
    private boolean isFrozen;

    // visible for the computeIfAbsent function to get
    final DataType cqlType;

    private CollectionCacheKey(
        int collectionProtoCode,
        int keyProtoCode,
        int valueProtoCode,
        boolean isFrozen,
        DataType cqlType) {
      this.collectionProtoCode = collectionProtoCode;
      this.keyProtoCode = keyProtoCode;
      this.valueProtoCode = valueProtoCode;
      this.isFrozen = isFrozen;
      this.cqlType = cqlType;
    }

    static Optional<CollectionCacheKey> maybeCreate(DataType cqlType) {
      return switch (cqlType) {
        case MapType mt ->
            Optional.of(
                new CollectionCacheKey(
                    mt.getProtocolCode(),
                    mt.getKeyType().getProtocolCode(),
                    mt.getValueType().getProtocolCode(),
                    mt.isFrozen(),
                    cqlType));
        case ListType lt ->
            Optional.of(
                new CollectionCacheKey(
                    lt.getProtocolCode(),
                    -1,
                    lt.getElementType().getProtocolCode(),
                    lt.isFrozen(),
                    cqlType));
        case SetType st ->
            Optional.of(
                new CollectionCacheKey(
                    st.getProtocolCode(),
                    -1,
                    st.getElementType().getProtocolCode(),
                    st.isFrozen(),
                    cqlType));
        default -> {
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                "CollectionCacheKey does not support supplied CQL type: {}",
                cqlType.asCql(true, true));
          }
          yield Optional.empty();
        }
      };
    }

    /**
     * We only need to check the protocol codes and frozen status, cqltype is only there for the
     * computeIfAbsent()
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      CollectionCacheKey that = (CollectionCacheKey) obj;
      return collectionProtoCode == that.collectionProtoCode
          && keyProtoCode == that.keyProtoCode
          && valueProtoCode == that.valueProtoCode
          && isFrozen == that.isFrozen;
    }

    /**
     * We only need to check the protocol codes and frozen status, cqltype is only there for the
     * computeIfAbsent()
     */
    @Override
    public int hashCode() {
      return Objects.hash(collectionProtoCode, keyProtoCode, valueProtoCode, isFrozen);
    }
  }
}
