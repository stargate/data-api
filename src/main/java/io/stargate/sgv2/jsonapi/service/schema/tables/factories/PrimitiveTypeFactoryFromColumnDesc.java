package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.PRIMITIVE_TYPES;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataTypeDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory for creating a primitive {@link ApiDataType} from a {@link ColumnDesc} provided by the
 * user
 *
 * <p>...
 */
public class PrimitiveTypeFactoryFromColumnDesc
    extends TypeFactoryFromColumnDesc<ApiDataType, ColumnDesc> {

  static final Map<ApiTypeName, PrimitiveTypeFactoryFromColumnDesc> ALL_FACTORIES;

  static {
    ALL_FACTORIES =
        Collections.unmodifiableMap(
            PRIMITIVE_TYPES.stream()
                .collect(
                    Collectors.toMap(
                        PrimitiveApiDataTypeDef::typeName,
                        PrimitiveTypeFactoryFromColumnDesc::new)));
  }

  private final PrimitiveApiDataTypeDef primitiveTypeInstance;

  private PrimitiveTypeFactoryFromColumnDesc(PrimitiveApiDataTypeDef primitiveType) {
    super(primitiveType.typeName(), ColumnDesc.class);
    this.primitiveTypeInstance = primitiveType;
  }

  @Override
  public ApiDataType create(
      TypeBindingPoint typeBindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize)
      throws UnsupportedUserType {
    return primitiveTypeInstance;
  }

  @Override
  public boolean isSupported(
      TypeBindingPoint typeBindingPoint,
      ColumnDesc columnDesc,
      VectorizeConfigValidator validateVectorize) {
    // primitive types are always supported
    return true;
  }
}
