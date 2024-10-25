package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ComplexApiDataType;
import java.util.Objects;

/** Interface for complex column types like collections */
public abstract class ComplexColumnDesc implements ColumnDesc {

  private final ApiDataTypeName apiDataTypeName;

  protected ComplexColumnDesc(ApiDataTypeName apiDataTypeName) {
    this.apiDataTypeName =
        Objects.requireNonNull(apiDataTypeName, "apiDataTypeName must not be null");
  }

  @Override
  public ApiDataTypeName getApiDataTypeName() {
    return apiDataTypeName;
  }

  /** Column type for {@link ComplexApiDataType.ApiMapType} */
  public static class MapColumnDesc extends ComplexColumnDesc {
    private final ColumnDesc keyType;
    private final ColumnDesc valueType;

    public MapColumnDesc(ColumnDesc keyType, ColumnDesc valueType) {
      super(ApiDataTypeName.MAP);

      this.keyType = keyType;
      this.valueType = valueType;
    }

    public ColumnDesc keyType() {
      return keyType;
    }

    public ColumnDesc valueType() {
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
      MapColumnDesc mapType = (MapColumnDesc) o;
      return Objects.equals(keyType, mapType.keyType)
          && Objects.equals(valueType, mapType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyType, valueType);
    }
  }

  /** Column type for {@link ComplexApiDataType.ApiListType} */
  public static class ListColumnDesc extends ComplexColumnDesc {
    private final ColumnDesc valueType;

    public ListColumnDesc(ColumnDesc valueType) {
      super(ApiDataTypeName.LIST);
      this.valueType = valueType;
    }

    public ColumnDesc valueType() {
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
      ListColumnDesc listType = (ListColumnDesc) o;
      return Objects.equals(valueType, listType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(valueType);
    }
  }

  /** Column type for {@link ComplexApiDataType.ApiSetType} */
  public static class SetColumnDesc extends ComplexColumnDesc {
    private final ColumnDesc valueType;

    public SetColumnDesc(ColumnDesc valueType) {
      super(ApiDataTypeName.SET);
      this.valueType = valueType;
    }

    public ColumnDesc valueType() {
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
      SetColumnDesc listType = (SetColumnDesc) o;
      return Objects.equals(valueType, listType.valueType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(valueType);
    }
  }

  /** Column type for {@link ComplexApiDataType.ApiVectorType} */
  public static class VectorColumnDesc extends ComplexColumnDesc {
    // Float will be default type for vector
    private final ColumnDesc valueType;
    private final int dimensions;
    private final VectorizeConfig vectorConfig;

    public VectorColumnDesc(ColumnDesc valueType, int dimensions, VectorizeConfig vectorConfig) {
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

    public ColumnDesc valueType() {
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
      VectorColumnDesc that = (VectorColumnDesc) o;
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
  public static class UnsupportedType implements ColumnDesc {

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
