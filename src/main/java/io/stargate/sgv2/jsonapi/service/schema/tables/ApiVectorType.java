package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ApiSupportDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.VectorColumnDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiVectorType extends CollectionApiDataType<VectorType> {

  public static final TypeFactoryFromColumnDesc<ApiVectorType, VectorColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiVectorType, VectorType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  // NOTE: the vector type cannot be frozen so we do not need different support for frozen
  // Update operators $set and $unset can be used for vector columns, but $push and $pullAll cannot.
  public static final ApiSupportDef API_SUPPORT =
      new ApiSupportDef.Support(
          true, ApiSupportDef.Collection.NONE, true, true, false, ApiSupportDef.Update.PRIMITIVE);

  private final int dimension;
  private final VectorizeDefinition vectorizeDefinition;

  private ApiVectorType(int dimension, VectorizeDefinition vectorizeDefinition) {
    super(
        ApiTypeName.VECTOR,
        ApiDataTypeDefs.FLOAT,
        new ExtendedVectorType(ApiDataTypeDefs.FLOAT.cqlType(), dimension),
        API_SUPPORT);
    this.dimension = dimension;
    this.vectorizeDefinition = vectorizeDefinition;
  }

  @Override
  public boolean isFrozen() {
    return false;
  }

  @Override
  public ColumnDesc columnDesc() {
    return new VectorColumnDesc(
        dimension,
        vectorizeDefinition == null ? null : vectorizeDefinition.toVectorizeConfig(),
        ApiSupportDesc.from(this));
  }

  public int getDimension() {
    return dimension;
  }

  public static boolean isDimensionSupported(int dimension) {
    return dimension > 0;
  }

  /**
   * @return Nullable VectorizeDefinition
   */
  public VectorizeDefinition getVectorizeDefinition() {
    return vectorizeDefinition;
  }

  public static ApiVectorType from(int dimension) {
    return from(dimension, null);
  }

  public static ApiVectorType from(int dimension, VectorizeDefinition vectorizeDefinition) {

    // Sanity check
    if (!isDimensionSupported(dimension)) {
      throw new IllegalArgumentException(
          "dimensions is not supported, dimensions=%s".formatted(dimension));
    }
    return new ApiVectorType(dimension, vectorizeDefinition);
  }

  private static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType == ApiDataTypeDefs.FLOAT;
  }

  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiVectorType, VectorColumnDesc> {

    @Override
    public ApiVectorType create(
        VectorColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      // this will catch the dimension aetc
      if (!isSupported(columnDesc, validateVectorize)) {
        throw new UnsupportedUserType(columnDesc);
      }

      Integer dimension;

      // if the vectorize config is specified, we validate the dimension or auto populate the
      // default dimension
      // the same logic as the collection
      if (columnDesc.getVectorizeConfig() != null) {
        dimension =
            validateVectorize.validateService(
                columnDesc.getVectorizeConfig(), columnDesc.getDimension());
      } else {
        // if the vectorize config is not specified, the dimension must be specified
        // the same logic as the collection
        if (columnDesc.getDimension() == null) {
          throw SchemaException.Code.MISSING_DIMENSION_IN_VECTOR_COLUMN.get();
        }
        dimension = columnDesc.getDimension();
      }

      var vectorDefn = VectorizeDefinition.from(columnDesc, dimension, validateVectorize);

      return ApiVectorType.from(dimension, vectorDefn);
    }

    @Override
    public boolean isSupported(
        VectorColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      return columnDesc.valueType().equals(PrimitiveColumnDesc.FLOAT);
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
      // we can ignore the element type, it is always float, checked in isSupported
      return ApiVectorType.from(cqlType.getDimensions(), vectorizeDefn);
    }

    @Override
    public boolean isSupported(VectorType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // Must be a float
      return cqlType.getElementType() == DataTypes.FLOAT;
    }
  }
}
