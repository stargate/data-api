package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.CqlTypeKey;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.TypeFactoryFromColumnDesc;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.TypeFactoryFromCql;
import java.util.Objects;
import java.util.Optional;

public class ApiVectorType extends CollectionApiDataType<VectorType> {

  public static final TypeFactoryFromColumnDesc<ApiVectorType, VectorColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();

  public static final TypeFactoryFromCql<ApiVectorType, VectorType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  // Here so the ApiVectorColumnDesc can get it when deserializing from JSON
  // NOTE: the vector type cannot be frozen so we do not need different support for frozen
  // Update operators $set and $unset can be used for vector columns, but $push and $pullAll cannot.
  public static final ApiSupportDef API_SUPPORT =
      new ApiSupportDef.Support(true, true, true, false, ApiSupportDef.Update.PRIMITIVE);

  private final int dimension;
  private final VectorizeDefinition vectorizeDefinition;

  /** NO VALIDATION - Testing Only */
  @VisibleForTesting
  public ApiVectorType(int dimension, VectorizeDefinition vectorizeDefinition) {
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
  public ColumnDesc getSchemaDescription(SchemaDescSource schemaDescSource) {
    // no different representation of the vector
    return new VectorColumnDesc(
        schemaDescSource,
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

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    var builder = super.recordTo(dataRecorder);
    builder.append("dimension", dimension);
    builder.append("vectorizeDefinition", vectorizeDefinition);
    return builder;
  }

  /**
   * Factory to create {@link ApiVectorType} from {@link VectorColumnDesc} obtained from user
   *
   * <p>...
   */
  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiVectorType, VectorColumnDesc> {

    private ColumnDescFactory() {
      super(ApiTypeName.VECTOR, VectorColumnDesc.class);
    }

    @Override
    public ApiVectorType create(
        TypeBindingPoint bindingPoint,
        VectorColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      if (!isTypeBindable(bindingPoint, columnDesc, validateVectorize)) {
        // TODO: XXX: AARON: NEED A general schema error ?
        throw new UnsupportedUserType(bindingPoint, columnDesc, (SchemaException) null);
      }

      Integer dimension;

      // if the vectorize config is specified, we validate the dimension or autopopulate the
      // default dimension
      if (columnDesc.getVectorizeConfig() != null) {
        dimension =
            validateVectorize.validateService(
                columnDesc.getVectorizeConfig(), columnDesc.getDimension());
      } else {
        // if the vectorize config is not specified, the dimension must be specified
        if (columnDesc.getDimension() == null) {
          throw SchemaException.Code.MISSING_DIMENSION_IN_VECTOR_COLUMN.get();
        }
        dimension = columnDesc.getDimension();
      }

      if (!isDimensionSupported(dimension)) {
        throw new UnsupportedUserType(
            bindingPoint,
            columnDesc,
            SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION.get(
                "unsupportedValue", columnDesc.getDimension().toString()));
      }

      var vectorDefn = VectorizeDefinition.from(columnDesc, dimension, validateVectorize);

      return new ApiVectorType(dimension, vectorDefn);
    }

    @Override
    public boolean isTypeBindable(
        TypeBindingPoint bindingPoint,
        VectorColumnDesc columnDesc,
        VectorizeConfigValidator validateVectorize) {

      // Currently no way for the user to specify a vector value type, just being safe
      if (!columnDesc.valueType().equals(PrimitiveColumnDesc.FLOAT)) {
        return false;
      }

      // not checking vector dimension, only check if the types are bindable here

      // can we use a vector of any type in this binding point?
      if (!SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromUser()) {
        return false;
      }
      return true;
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint) {
      return SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromUser();
    }
  }

  /**
   * Type factory to create {@link ApiVectorType} from {@link VectorType} obtained from driver /
   * cql.
   *
   * <p>...
   */
  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiVectorType, VectorType> {

    public CqlTypeFactory() {
      // see VectorType - this is a custom type for now.
      super(ProtocolConstants.DataType.CUSTOM, VectorType.class);
    }

    @Override
    public ApiVectorType create(
        TypeBindingPoint bindingPoint, VectorType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isTypeBindable(bindingPoint, cqlType)) {
        throw new UnsupportedCqlType(bindingPoint, cqlType);
      }

      // we can ignore the element type, it is always float, checked in isSupported
      return new ApiVectorType(cqlType.getDimensions(), vectorizeDefn);
    }

    @Override
    public boolean isTypeBindable(TypeBindingPoint bindingPoint, VectorType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // Must be a float, in reality they all are but checking
      if (cqlType.getElementType() != DataTypes.FLOAT) {
        return false;
      }

      if (!isDimensionSupported(cqlType.getDimensions())) {
        throw new IllegalArgumentException(
            "dimensions is not supported, dimensions=%s".formatted(cqlType.getDimensions()));
      }

      // can we use a vector of any type in this binding point?
      if (!SUPPORT_BINDING_RULES.rule(bindingPoint).bindableFromDb()) {
        return false;
      }

      // NOTE: not checking if the float / element type can be used as a collection value
      // because all vector types are always float
      return true;
    }

    @Override
    public Optional<CqlTypeKey> maybeCreateCacheKey(
        TypeBindingPoint bindingPoint, VectorType cqlType) {

      // We cannot cache any vector type as it has user defined dimension and vectorize config
      return Optional.empty();
    }
  }
}
