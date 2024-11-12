package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypeFactoryFromColumnDesc<ApiT extends ApiDataType, DescT extends ColumnDesc>
    extends FactoryFromDesc {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactoryFromColumnDesc.class);

  public static final TypeFactoryFromColumnDesc<ApiDataType, ColumnDesc> DEFAULT =
      new DefaultFactory();

  public abstract ApiT create(DescT columnDesc, VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType;

  public abstract boolean isSupported(DescT columnDesc, VectorizeConfigValidator validateVectorize);

  public UnsupportedApiDataType createUnsupported(DescT columnDesc) {
    return new UnsupportedUserApiDataType(columnDesc);
  }

  private static class DefaultFactory extends TypeFactoryFromColumnDesc<ApiDataType, ColumnDesc> {

    private static final Map<ApiTypeName, PrimitiveApiDataTypeDef> PRIMITIVE_TYPES_BY_API_NAME =
        PRIMITIVE_TYPES.stream()
            .collect(Collectors.toMap(PrimitiveApiDataTypeDef::typeName, Function.identity()));

    private static final ConcurrentMap<ColumnDesc, CollectionApiDataType> COLLECTION_TYPE_CACHE =
        new ConcurrentHashMap<>();

    @Override
    public ApiDataType create(ColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      var primitiveType = PRIMITIVE_TYPES_BY_API_NAME.get(columnDesc.typeName());
      if (primitiveType != null) {
        return primitiveType;
      }

      // Do not cache the Vector type because the ApiVectorType has the user defined vectorizarion
      // on it
      if (columnDesc instanceof VectorColumnDesc vt) {
        return ApiVectorType.FROM_COLUMN_DESC_FACTORY.create(vt, validateVectorize);
      }

      try {
        return COLLECTION_TYPE_CACHE.computeIfAbsent(
            columnDesc,
            entry -> {
              try {
                return switch (columnDesc) {
                  case ListColumnDesc lt ->
                      ApiListType.FROM_COLUMN_DESC_FACTORY.create(lt, validateVectorize);
                  case MapColumnDesc mt ->
                      ApiMapType.FROM_COLUMN_DESC_FACTORY.create(mt, validateVectorize);
                  case SetColumnDesc st ->
                      ApiSetType.FROM_COLUMN_DESC_FACTORY.create(st, validateVectorize);
                  default -> throw new UnsupportedUserType(columnDesc);
                };
              } catch (UnsupportedUserType e) {
                throw new RuntimeException(e);
              }
            });
      } catch (RuntimeException e) {
        if (e.getCause() instanceof UnsupportedUserType) {
          throw (UnsupportedUserType) e.getCause();
        }
        throw e;
      }
    }

    @Override
    public boolean isSupported(ColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      return true;
    }
  }
}
