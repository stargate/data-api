package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnDesc;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiVectorType extends CollectionApiDataType {

  public static final TypeFactoryFromColumnDesc<ApiVectorType, ComplexColumnDesc.VectorColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiVectorType, VectorType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  private final int dimension;
  private VectorizeDefinition vectorizeDefinition;

  private ApiVectorType(
      PrimitiveApiDataTypeDef valueType, int dimensions, VectorizeDefinition vectorizeDefinition) {
    super(
        ApiDataTypeName.VECTOR,
        valueType,
        new ExtendedVectorType(valueType.cqlType(), dimensions),
        new ComplexColumnDesc.VectorColumnDesc(
            valueType.columnDesc(),
            dimensions,
            vectorizeDefinition == null ? null : vectorizeDefinition.toVectorizeConfig()));

    this.dimension = dimensions;

    // sanity checking
    if (!isValueTypeSupported(valueType)) {
      throw new IllegalArgumentException("valueType is not supported");
    }
  }

  public int getDimension() {
    return dimension;
  }

  /**
   * @return Nullable VectorizeDefinition
   */
  public VectorizeDefinition getVectorizeDefinition() {
    return vectorizeDefinition;
  }

  public static ApiVectorType from(
      ApiDataType valueType, int dimensions, VectorizeDefinition vectorizeDefinition) {
    Objects.requireNonNull(valueType, "valueType must not be null");

    if (valueType instanceof PrimitiveApiDataTypeDef vtp) {
      return new ApiVectorType(vtp, dimensions, vectorizeDefinition);
    }
    throw new IllegalArgumentException(
        "valueType must be primitive type, valueType%s".formatted(valueType));
  }

  private static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType == ApiDataTypeDefs.FLOAT;
  }

  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiVectorType, ComplexColumnDesc.VectorColumnDesc> {

    @Override
    public ApiVectorType create(
        ComplexColumnDesc.VectorColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      ApiDataType valueType;
      try {
        valueType =
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize);
      } catch (UnsupportedUserType e) {
        throw new UnsupportedUserType(columnDesc, e);
      }

      // Not calling isSupported to avoid double decoding of the valueType
      if (isValueTypeSupported(valueType)) {
        var vectorDefn = VectorizeDefinition.from(columnDesc, validateVectorize);
        // TODO:  aaron - NOT SURE WHAT IS HAPPENING with VectorizeConfigValidator
        // It validates AND gets a new dimension
        var dimensions =
            validateVectorize.validateService(
                columnDesc.getVectorConfig(), columnDesc.getDimensions());
        return new io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType(
            (PrimitiveApiDataTypeDef) valueType, dimensions, vectorDefn);
      }
      throw new UnsupportedUserType(columnDesc);
    }

    @Override
    public boolean isSupported(
        ComplexColumnDesc.VectorColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      try {
        return isValueTypeSupported(
            TypeFactoryFromColumnDesc.DEFAULT.create(columnDesc.valueType(), validateVectorize));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }
  }

  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiVectorType, VectorType> {

    @Override
    public ApiVectorType create(VectorType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(cqlType)) {
        throw new UnsupportedCqlType(cqlType);
      }

      try {
        var valueType = TypeFactoryFromCql.DEFAULT.create(cqlType.getElementType(), vectorizeDefn);
        return ApiVectorType.from(valueType, cqlType.getDimensions(), vectorizeDefn);

      } catch (UnsupportedCqlType e) {
        // should not happen if the isCqlTypeSupported returns true
        throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
      }
    }

    @Override
    public boolean isSupported(VectorType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // Must be a float
      return cqlType.getElementType() == DataTypes.FLOAT;
    }
  }
}
