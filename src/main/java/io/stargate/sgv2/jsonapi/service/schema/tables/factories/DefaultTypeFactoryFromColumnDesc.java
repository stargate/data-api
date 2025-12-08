package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;
import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
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

    // check we have a factory for every enum value
    for (ApiTypeName typeName : ApiTypeName.values()) {
      if (!factories.containsKey(typeName)) {
        throw new IllegalStateException(
            "DefaultTypeFactoryFromColumnDesc - missing TypeFactoryFromColumnDesc for " + typeName);
      }
    }
    ALL_FACTORIES = Collections.unmodifiableMap(factories);
  }

  /**
   * Use the singleton {@link #INSTANCE}
   *
   * <p>...
   */
  private DefaultTypeFactoryFromColumnDesc() {
    super(null, ColumnDesc.class); // No specific API type name for the default factory
  }

  /**
   * Gets a factory to create an {@link ApiDataType} from the given {@link ColumnDesc} parsed form
   * user input.
   *
   * <p>We must have a factory for every {@link ApiTypeName} that is supported by the API, we enfore
   * this so that any bugs in mapping are more likely to be caught.
   *
   * @param bindingPoint The location where the type is used.
   * @param columnDesc The column description that defines the type from user input.
   * @return A factory that can create the {@link ApiDataType} from the given {@link ColumnDesc}.
   * @throws IllegalStateException if there is no factory for the given {@link
   *     ColumnDesc#typeName()}.
   */
  public static TypeFactoryFromColumnDesc<? extends ApiDataType, ? extends ColumnDesc> factoryFor(
      TypeBindingPoint bindingPoint, ColumnDesc columnDesc) {

    var factory = ALL_FACTORIES.get(columnDesc.typeName());
    if (factory != null) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "DefaultTypeFactoryFromColumnDesc.factoryFor() - found factory bindingPoint: {}, columnDesc: {}, factory.class: {}",
            bindingPoint,
            columnDesc,
            classSimpleName(factory.getClass()));
      }
      return factory;
    }

    // we should never get here because we check this when we make the ALL_FACTORIES map
    throw new IllegalStateException(
        "DefaultTypeFactoryFromColumnDesc.factoryFor() - missing TypeFactoryFromColumnDesc for "
            + columnDesc.typeName());
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
    try {
      return typeFactory.createUntyped(bindingPoint, columnDesc, validateVectorize);
    } catch (UnsupportedUserType uut) {
      // this can be used to pass out a schema exception we want to pass to the user.
      if (uut.schemaException != null) {
        throw uut.schemaException;
      }
      throw uut;
    }
  }

  @Override
  public boolean isTypeBindable(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize) {

    return factoryFor(bindingPoint, columnDesc)
        .isTypeBindableUntyped(bindingPoint, columnDesc, validateVectorize);
  }

  /** As a default factory we do not support binding, only the real factories do. */
  @Override
  public boolean isTypeBindable(TypeBindingPoint bindingPoint) {
    return false;
  }

  /**
   * Returns all the {@link ApiTypeName} that can be bound to the given {@link TypeBindingPoint}.
   *
   * @param bindingPoint The binding point to check for bindable types.
   * @return List of supported types, sorted by {@link ApiTypeName#COMPARATOR}.
   */
  public List<ApiTypeName> allBindableTypes(TypeBindingPoint bindingPoint) {

    return ALL_FACTORIES.values().stream()
        .filter(factory -> factory.isTypeBindable(bindingPoint))
        .map(TypeFactoryFromColumnDesc::apiTypeName)
        .sorted(ApiTypeName.COMPARATOR)
        .toList();
  }
}
