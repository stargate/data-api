package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;

import com.datastax.oss.driver.api.core.type.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypeFactoryFromCql<ApiT extends ApiDataType, CqlT extends DataType> {
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
            .collect(Collectors.toMap(PrimitiveApiDataTypeDef::getCqlType, Function.identity()));

    private static final ConcurrentMap<DataType, CollectionApiDataType> COLLECTION_TYPE_CACHE =
        new ConcurrentHashMap<>();

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

      // Note the equals for a Driver MapType does not take the frozen flag into account that abould
      // be OK because
      // we will throw an error and not support frozen maps
      try {
        return COLLECTION_TYPE_CACHE.computeIfAbsent(
            cqlType,
            entry -> {
              try {
                return switch (cqlType) {
                  case MapType mt -> ApiMapType.FROM_CQL_FACTORY.create(mt, vectorizeDefn);
                  case ListType lt -> ApiListType.FROM_CQL_FACTORY.create(lt, vectorizeDefn);
                  case SetType st -> ApiSetType.FROM_CQL_FACTORY.create(st, vectorizeDefn);
                  default -> throw new UnsupportedCqlType(cqlType);
                };
              } catch (UnsupportedCqlType e) {
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
}
