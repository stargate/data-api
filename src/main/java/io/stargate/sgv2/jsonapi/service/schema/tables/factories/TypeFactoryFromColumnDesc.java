package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for factories that create {@link ApiDataType} from a {@link ColumnDesc} passed it from the
 * user.
 *
 * <p>
 *
 * @param <ApiT>
 * @param <DescT>
 */
public abstract class TypeFactoryFromColumnDesc<ApiT extends ApiDataType, DescT extends ColumnDesc>
    extends TypeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TypeFactoryFromColumnDesc.class);

  private final ApiTypeName apiTypeName;
  private final Class<DescT> descClass;

  protected TypeFactoryFromColumnDesc(ApiTypeName apiTypeName, Class<DescT> descClass) {
    this.apiTypeName = apiTypeName; // allow null for the default factory
    this.descClass = Objects.requireNonNull(descClass, "descClass must not be null");
  }

  /**
   * Called to get the name of the API type that this factory creates, so that decisions can be made
   * based on the type the factory creates.
   */
  public ApiTypeName apiTypeName() {
    return apiTypeName;
  }

  /** See {@link #create(TypeBindingPoint, ColumnDesc, VectorizeConfigValidator)} */
  public ApiT createUntyped(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType {

    var typedDesc =
        checkCastToChild("TypeFactoryFromColumnDesc.createUntyped()", descClass, columnDesc);
    return create(bindingPoint, typedDesc, validateVectorize);
  }

  /**
   * Called to create an {@link ApiDataType} from the user provided {@link ColumnDesc}
   *
   * <p>Implementations should check that the type desc is bindable and raise a {@link
   * io.stargate.sgv2.jsonapi.exception.SchemaException} if the usage is not allowed for the given
   * {@link TypeBindingPoint} and {@link ColumnDesc}. Wrapped it in the {@link UnsupportedUserType}
   * exception so that callers can handle it properly.
   *
   * @param bindingPoint The binding point where this type is being used.
   * @param columnDesc The user provided column description that defines the type.
   * @param validateVectorize The validator for vectorization configuration
   * @return A valid {@link ApiDataType} that represents the type the user wanted.
   * @throws UnsupportedUserType if the type is not allowed to be used at the binding point, see
   *     above for returning errors in the {@link UnsupportedUserType} exception.
   */
  public abstract ApiT create(
      TypeBindingPoint bindingPoint, DescT columnDesc, VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType;

  /** See {@link #isTypeBindable(TypeBindingPoint, ColumnDesc, VectorizeConfigValidator)} */
  public boolean isTypeBindableUntyped(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize) {

    var typedDesc =
        checkCastToChild(
            "TypeFactoryFromColumnDesc.isTypeBindableUntyped()", descClass, columnDesc);
    return isTypeBindable(bindingPoint, typedDesc, validateVectorize);
  }

  /**
   * Called to determine if the user provided definition of the type in {@link ColumnDesc} is
   * bindable to the given {@link TypeBindingPoint}.
   *
   * <p>Implementations should check that the type is allowed to be used at the binding point, and
   * that any components of the type are bindable. e.g. for a set type, the value type must be
   * bindable as a collection value. Call the {@link DefaultTypeFactoryFromColumnDesc#INSTANCE} to
   * test components of the type.
   *
   * @param bindingPoint The binding point where this type is being used.
   * @param columnDesc The user provided column description that defines the type.
   * @param validateVectorize The validator for vectorization configuration
   * @return true if the type can be bound to the given binding point, false otherwise.
   */
  public abstract boolean isTypeBindable(
      TypeBindingPoint bindingPoint, DescT columnDesc, VectorizeConfigValidator validateVectorize);

  /**
   * Called to test if the factory can create an instance of the type it creates for the given
   * {@link TypeBindingPoint} as the *user*. This is used to determine what possible types can be
   * used at a binging point for error messages, not to create an actual type.
   *
   * <p>Implementations only need to check if the factory can create the type it creates at the
   * binding point, because we do not have a user definition of the type we cannot check any
   * components of the type. e.g. we cannot check if the value-type of a list is bindable.
   *
   * @param bindingPoint The binding point where this type is being used.
   * @return
   */
  public abstract boolean isTypeBindable(TypeBindingPoint bindingPoint);

  public UnsupportedApiDataType createUnsupported(DescT columnDesc) {
    return new UnsupportedUserApiDataType(columnDesc);
  }
}
