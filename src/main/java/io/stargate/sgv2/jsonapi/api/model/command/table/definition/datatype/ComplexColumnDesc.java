package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.Objects;

/** Interface for complex column types like collections */
public abstract class ComplexColumnDesc implements ColumnDesc {

  private final ApiDataTypeName apiDataTypeName;
  private final ApiSupportDesc apiSupportDesc;

  protected ComplexColumnDesc(ApiDataTypeName apiDataTypeName, ApiSupportDesc apiSupportDesc) {
    this.apiDataTypeName =
        Objects.requireNonNull(apiDataTypeName, "apiDataTypeName must not be null");
    this.apiSupportDesc = Objects.requireNonNull(apiSupportDesc, "apiSupportDesc must not be null");;
  }

  @Override
  public ApiDataTypeName typeName() {
    return apiDataTypeName;
  }

  @Override
  public ApiSupportDesc apiSupport() {
    return apiSupportDesc;
  }

  /** Column type for {@link ApiMapType} */
  public static class MapColumnDesc extends ComplexColumnDesc {
    private final ColumnDesc keyType;
    private final ColumnDesc valueType;

    public MapColumnDesc(ColumnDesc keyType, ColumnDesc valueType) {
      super(ApiDataTypeName.MAP, ApiSupportDesc.fullSupport(""));

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

  /** Column type for {@link ApiListType} */
  public static class ListColumnDesc extends ComplexColumnDesc {
    private final ColumnDesc valueType;

    public ListColumnDesc(ColumnDesc valueType) {
      super(ApiDataTypeName.LIST, ApiSupportDesc.fullSupport(""));
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

  /** Column type for {@link ApiSetType} */
  public static class SetColumnDesc extends ComplexColumnDesc {
    private final ColumnDesc valueType;

    public SetColumnDesc(ColumnDesc valueType) {
      super(ApiDataTypeName.SET, ApiSupportDesc.fullSupport(""));
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

  /** Column type for {@link ApiVectorType} */
  public static class VectorColumnDesc extends ComplexColumnDesc {
    // Float will be default type for vector
    private final ColumnDesc valueType;
    private final int dimensions;
    private final VectorizeConfig vectorConfig;

    public VectorColumnDesc(ColumnDesc valueType, int dimensions, VectorizeConfig vectorConfig) {
      super(ApiDataTypeName.VECTOR, ApiSupportDesc.fullSupport(""));

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
}
