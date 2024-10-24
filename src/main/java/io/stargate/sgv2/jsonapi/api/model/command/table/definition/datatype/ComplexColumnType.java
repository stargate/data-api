package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ComplexApiDataType;
import java.util.Objects;

/** Interface for complex column types like collections */
public abstract class ComplexColumnType implements ColumnType {

  private final ApiDataTypeName apiDataTypeName;

  protected ComplexColumnType(ApiDataTypeName apiDataTypeName) {
    this.apiDataTypeName =
        Objects.requireNonNull(apiDataTypeName, "apiDataTypeName must not be null");
  }

  @Override
  public ApiDataTypeName getApiDataTypeName() {
    return apiDataTypeName;
  }

  /** Column type for {@link ComplexApiDataType.ApiMapType} */
  public static class ColumnMapType extends ComplexColumnType {
    private final ColumnType keyType;
    private final ColumnType valueType;

    public ColumnMapType(ColumnType keyType, ColumnType valueType) {
      super(ApiDataTypeName.MAP);

      this.keyType = keyType;
      this.valueType = valueType;
    }

    public ColumnType keyType() {
      return keyType;
    }

    public ColumnType valueType() {
      return valueType;
    }

    // Needed for testing
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnMapType mapType = (ColumnMapType) o;
      return Objects.equals(keyType, mapType.keyType)
          && Objects.equals(valueType, mapType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyType, valueType);
    }
  }

  /** Column type for {@link ComplexApiDataType.ApiListType} */
  public static class ColumnListType extends ComplexColumnType {
    private final ColumnType valueType;

    public ColumnListType(ColumnType valueType) {
      super(ApiDataTypeName.LIST);
      this.valueType = valueType;
    }

    public ColumnType valueType() {
      return valueType;
    }

    // Needed for testing
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnListType listType = (ColumnListType) o;
      return Objects.equals(valueType, listType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(valueType);
    }
  }

  /** Column type for {@link ComplexApiDataType.ApiSetType} */
  public static class ColumnSetType extends ComplexColumnType {
    private final ColumnType valueType;

    public ColumnSetType(ColumnType valueType) {
      super(ApiDataTypeName.SET);
      this.valueType = valueType;
    }

    public ColumnType valueType() {
      return valueType;
    }

    // Needed for testing
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnSetType listType = (ColumnSetType) o;
      return Objects.equals(valueType, listType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(valueType);
    }
  }

  /** Column type for {@link ComplexApiDataType.ApiVectorType} */
  public static class ColumnVectorType extends ComplexColumnType {
    // Float will be default type for vector
    private final ColumnType valueType;
    private final int dimensions;
    private final VectorizeConfig vectorConfig;

    public ColumnVectorType(ColumnType valueType, int dimensions, VectorizeConfig vectorConfig) {
      super(ApiDataTypeName.VECTOR);

      this.valueType = valueType;
      this.dimensions = dimensions;
      this.vectorConfig = vectorConfig;
    }

    public VectorizeConfig getVectorConfig() {
      return vectorConfig;
    }

    public int getDimensions() {
      return dimensions;
    }

    public ColumnType valueType() {
      return valueType;
    }

    // Needed for testing
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnVectorType that = (ColumnVectorType) o;
      return dimensions == that.dimensions
          && Objects.equals(valueType, that.valueType)
          && Objects.equals(vectorConfig, that.vectorConfig);
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueType, dimensions, vectorConfig);
    }
  }

  /**
   * Unsupported type implementation, returned in response when cql table has unsupported format
   * column
   */
  public static class UnsupportedType implements ColumnType {

    public static final String UNSUPPORTED_TYPE_NAME = "UNSUPPORTED";
    private final String cqlFormat;

    public UnsupportedType(DataType cqlType) {
      this.cqlFormat = cqlType.asCql(true, true);
    }

    @Override
    public ApiDataTypeName getApiDataTypeName() {
      throw new UnsupportedOperationException("Unsupported type");
    }

    @Override
    public String getApiName() {
      return UNSUPPORTED_TYPE_NAME;
    }

    public String cqlFormat() {
      return cqlFormat;
    }
  }
}
