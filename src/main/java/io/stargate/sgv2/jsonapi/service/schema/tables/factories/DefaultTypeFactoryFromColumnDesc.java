package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ... */
public class DefaultTypeFactoryFromColumnDesc
    extends TypeFactoryFromColumnDesc<ApiDataType, ColumnDesc> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultTypeFactoryFromColumnDesc.class);

  public static final DefaultTypeFactoryFromColumnDesc INSTANCE =
      new DefaultTypeFactoryFromColumnDesc();

  private static final Map<
          ApiTypeName, TypeFactoryFromColumnDesc<? extends ApiDataType, ? extends ColumnDesc>>
      ALL_FACTORIES;

  static {
    EnumMap<ApiTypeName, TypeFactoryFromColumnDesc<? extends ApiDataType, ? extends ColumnDesc>>
        factories = new EnumMap<>(ApiTypeName.class);

    addFactory(factories, ApiVectorType.FROM_COLUMN_DESC_FACTORY);
    addFactory(factories, ApiUdtType.FROM_COLUMN_DESC_FACTORY);
    addFactory(factories, ApiMapType.FROM_COLUMN_DESC_FACTORY);
    addFactory(factories, ApiListType.FROM_COLUMN_DESC_FACTORY);
    addFactory(factories, ApiSetType.FROM_COLUMN_DESC_FACTORY);

    PRIMITIVE_TYPES.forEach(
        primitiveType ->
            addFactory(factories, new PrimitiveTypeFactoryFromColumnDesc(primitiveType)));
    ALL_FACTORIES = Collections.unmodifiableMap(factories);
  }

  DefaultTypeFactoryFromColumnDesc() {
    super(null, ColumnDesc.class); // No specific API type name for the default factory
  }

  public static TypeFactoryFromColumnDesc<? extends ApiDataType, ? extends ColumnDesc> factoryFor(
      TypeBindingPoint bindingPoint, ColumnDesc columnDesc) throws UnsupportedUserType {

    var factory = ALL_FACTORIES.get(columnDesc.typeName());
    if (factory != null) {
      return factory;
    }

    // Unlike DefaultTypeFactoryFromCql this *may* happen, it could be the user fat-fingered the
    // type name

    // TODO: XXX: AARON: need a schema exception here
    throw new UnsupportedUserType(bindingPoint, columnDesc, (SchemaException) null);
  }

  private static void addFactory(
      Map<ApiTypeName, TypeFactoryFromColumnDesc<? extends ApiDataType, ? extends ColumnDesc>> map,
      TypeFactoryFromColumnDesc<? extends ApiDataType, ? extends ColumnDesc> factory) {
    map.put(factory.apiTypeName(), factory);
  }

  @Override
  public ApiTypeName apiTypeName() {
    throw new UnsupportedOperationException(
        "DefaultTypeFactoryFromColumnDesc does not have a specific API type name");
  }

  /**
   * Creates an {@link ApiDataType} from the given {@link ColumnDesc}.
   *
   * <p>Unlike {@link DefaultTypeFactoryFromCql} we do not cache any of the {@link ApiDataType} we
   * create here because:
   *
   * <ul>
   *   <li>This only runs for the incoming create DDL requests, so it is not performance critical.
   *   <li>The cache of types would not be shared with the {@DefaultTypeFactoryFromCql} which is
   *       used to represent the CQL schema as is stored in the DB, which is the representation of
   *       the schema we cache and used for all other incoming requests.
   *   <li>The {@link ApiUdtShallowType} shows that for an incoming create DDL command we are not
   *       building a full schema object, so to avoid more complexity we should only build that from
   *       the DB.
   * </ul>
   */
  @Override
  public ApiDataType create(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType {
    Objects.requireNonNull(columnDesc, "columnDesc must not be null");

    // throws if we cannot find a factory for the typeName, meaning we have no way to support it
    var typeFactory = factoryFor(bindingPoint, columnDesc);

    // throws if it cannot create, meaning this config of the type is not supported.
    // caller needs to check if the type usage is supported, such as create table, etc.
    return typeFactory.createUntyped(bindingPoint, columnDesc, validateVectorize);

    //      var primitiveType = PRIMITIVE_TYPES_BY_API_NAME.get(columnDesc.typeName());
    //      if (primitiveType != null) {
    //        // this will catch things like the counter type
    //        if (!primitiveType.apiSupport().createTable()) {
    //          throw new UnsupportedUserType(columnDesc);
    //        }
    //        return primitiveType;
    //      }
    //
    //      // Do not cache the Vector type because the ApiVectorType has the user defined
    // vectorizarion
    //      // on it
    //      if (columnDesc instanceof VectorColumnDesc vt) {
    //        return ApiVectorType.FROM_COLUMN_DESC_FACTORY.create(vt, validateVectorize);
    //      }
    //      // Do not cache the UDT type because it can has same name but different definition.
    //      if (columnDesc instanceof UDTRefColumnDesc udt) {
    //        return ApiUdtType.FROM_COLUMN_DESC_FACTORY.create(udt, validateVectorize);
    //      }
    //
    //      try {
    //        return COLLECTION_TYPE_CACHE.computeIfAbsent(
    //            columnDesc,
    //            entry -> {
    //              try {
    //                return switch (columnDesc) {
    //                  case ListColumnDesc lt ->
    //                      ApiListType.FROM_COLUMN_DESC_FACTORY.create(lt, validateVectorize);
    //                  case MapColumnDesc mt ->
    //                      ApiMapType.FROM_COLUMN_DESC_FACTORY.create(mt, validateVectorize);
    //                  case SetColumnDesc st ->
    //                      ApiSetType.FROM_COLUMN_DESC_FACTORY.create(st, validateVectorize);
    //                  default -> throw new UnsupportedUserType(columnDesc);
    //                };
    //              } catch (UnsupportedUserType e) {
    //                throw new RuntimeException(e);
    //              }
    //            });
    //      } catch (RuntimeException e) {
    //        if (e.getCause() instanceof UnsupportedUserType) {
    //          throw (UnsupportedUserType) e.getCause();
    //        }
    //        throw e;
    //      }
  }

  @Override
  public boolean isSupported(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize) {

    // throws if we cannot find a factory for the columnDesc
    try {
      return factoryFor(bindingPoint, columnDesc)
          .isSupportedUntyped(bindingPoint, columnDesc, validateVectorize);
    } catch (UnsupportedUserType e) {
      // if we could not find a factory for the cqlType, then it is not supported
      return false;
    }
  }
}
