package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataTypeDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;

/**
 * Factory for creating a primitive {@link ApiDataType} from a {@link ColumnDesc} provided by the
 * user
 *
 * <p>...
 */
public class PrimitiveTypeFactoryFromColumnDesc
    extends TypeFactoryFromColumnDesc<ApiDataType, ColumnDesc> {

  private final PrimitiveApiDataTypeDef primitiveTypeInstance;

  PrimitiveTypeFactoryFromColumnDesc(PrimitiveApiDataTypeDef primitiveType) {
    super(primitiveType.typeName(), ColumnDesc.class);
    this.primitiveTypeInstance = primitiveType;
  }

  @Override
  public ApiDataType create(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType {

    if (!isSupported(bindingPoint, columnDesc, validateVectorize)) {
      // TODO: XXX: AARON need a general schema exception ?
      throw new UnsupportedUserType(bindingPoint, columnDesc, (SchemaException) null);
    }
    return primitiveTypeInstance;
  }

  @Override
  public boolean isSupported(
      TypeBindingPoint bindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize) {

    return primitiveTypeInstance.supportBindingRules().rule(bindingPoint).supportedFromUser();
  }
}
