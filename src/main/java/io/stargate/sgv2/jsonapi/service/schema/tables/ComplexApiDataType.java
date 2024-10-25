package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import java.util.Objects;

/** Interface defining the api data type for complex types */
public abstract class ComplexApiDataType implements ApiDataType {
  private final ApiDataTypeName typeName;
  private final PrimitiveApiDataTypeDef valueType;
  private final DataType cqlType;
  private final ColumnDesc columnDesc;

  protected ComplexApiDataType(
      ApiDataTypeName typeName,
      PrimitiveApiDataTypeDef valueType,
      DataType cqlType,
      ColumnDesc columnDesc) {
    this.typeName = typeName;
    this.valueType = valueType;
    this.cqlType = cqlType;
    this.columnDesc = columnDesc;
  }

  @Override
  public ApiDataTypeName getName() {
    return typeName;
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public boolean isContainer() {
    return true;
  }

  @Override
  public boolean isUnsupported() {
    return true;
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public ColumnDesc getColumnType() {
    return columnDesc;
  }

  public PrimitiveApiDataTypeDef getValueType() {
    return valueType;
  }

  // ===================================================================================================================
  // MapType
  // ===================================================================================================================

  public static class ApiMapType extends ComplexApiDataType {

    private final PrimitiveApiDataTypeDef keyType;

    public ApiMapType(PrimitiveApiDataTypeDef keyType, PrimitiveApiDataTypeDef valueType) {
      super(
          ApiDataTypeName.MAP,
          valueType,
          DataTypes.mapOf(keyType.getCqlType(), valueType.getCqlType()),
          new ComplexColumnDesc.MapColumnDesc(keyType.getColumnType(), valueType.getColumnType()));

      this.keyType = keyType;
      // sanity checking
      if (!isKeyTypeSupported(keyType)) {
        throw new IllegalArgumentException("keyType is not supported");
      }
      if (!isValueTypeSupported(valueType)) {
        throw new IllegalArgumentException("valueType is not supported");
      }
    }

    public static ApiMapType from(ApiDataType keyType, ApiDataType valueType) {
      Objects.requireNonNull(keyType, "keyType must not be null");
      Objects.requireNonNull(valueType, "valueType must not be null");

      if (isKeyTypeSupported(keyType) && isValueTypeSupported(valueType)) {
        return new ApiMapType(
            (PrimitiveApiDataTypeDef) keyType, (PrimitiveApiDataTypeDef) valueType);
      }
      throw new IllegalArgumentException(
          "keyType and valueType must be primitive types, keyType: %s valueType: %s"
              .formatted(keyType, valueType));
    }

    public static ApiMapType from(ComplexColumnDesc.MapColumnDesc mapType)
        throws UnsupportedUserType {
      Objects.requireNonNull(mapType, "mapType must not be null");

      var keyType = ApiDataTypeDefs.from(mapType.keyType());
      var valueType = ApiDataTypeDefs.from(mapType.valueType());

      if (isKeyTypeSupported(keyType) && isValueTypeSupported(valueType)) {
        return ApiMapType.from(keyType, valueType);
      }
      throw new UnsupportedUserType(mapType);
    }

    public static boolean isCqlTypeSupported(MapType cqlMapType) {
      Objects.requireNonNull(cqlMapType, "cqlMapType must not be null");

      // cannot be frozen
      if (cqlMapType.isFrozen()) {
        return false;
      }
      // keys must be text or ascii, because keys in JSON are string
      if (!(cqlMapType.getKeyType() == DataTypes.TEXT
          || cqlMapType.getKeyType() == DataTypes.ASCII)) {
        return false;
      }
      // must be a primitive type value
      return cqlMapType.getValueType() instanceof PrimitiveType;
    }

    public static boolean isKeyTypeSupported(ApiDataType keyType) {
      Objects.requireNonNull(keyType, "keyType must not be null");

      // keys must be text or ascii, because keys in JSON are string
      return keyType == ApiDataTypeDefs.ASCII || keyType == ApiDataTypeDefs.TEXT;
    }

    public static boolean isValueTypeSupported(ApiDataType valueType) {
      Objects.requireNonNull(valueType, "valueType must not be null");

      return valueType.isPrimitive();
    }

    public static boolean isColumnTypeSupported(ComplexColumnDesc.MapColumnDesc mapType) {
      Objects.requireNonNull(mapType, "mapType must not be null");

      try {
        return isKeyTypeSupported(ApiDataTypeDefs.from(mapType.keyType()))
            && isValueTypeSupported(ApiDataTypeDefs.from(mapType.valueType()));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }

    public PrimitiveApiDataTypeDef getKeyType() {
      return keyType;
    }
  }

  // ===================================================================================================================
  // ListType
  // ===================================================================================================================
  public static class ApiListType extends ComplexApiDataType {

    public ApiListType(PrimitiveApiDataTypeDef valueType) {
      super(
          ApiDataTypeName.LIST,
          valueType,
          DataTypes.listOf(valueType.getCqlType()),
          new ComplexColumnDesc.ListColumnDesc(valueType.getColumnType()));

      // sanity checking
      if (!isValueTypeSupported(valueType)) {
        throw new IllegalArgumentException("valueType is not supported");
      }
    }

    public static ApiListType from(ApiDataType valueType) {
      Objects.requireNonNull(valueType, "valueType must not be null");

      if (isValueTypeSupported(valueType)) {
        return new ApiListType((PrimitiveApiDataTypeDef) valueType);
      }
      throw new IllegalArgumentException(
          "valueType must be primitive type, valueType%s".formatted(valueType));
    }

    public static ApiListType from(ComplexColumnDesc.ListColumnDesc listType)
        throws UnsupportedUserType {
      Objects.requireNonNull(listType, "listType must not be null");

      var valueType = ApiDataTypeDefs.from(listType.valueType());
      if (isValueTypeSupported(valueType)) {
        return new ApiListType((PrimitiveApiDataTypeDef) valueType);
      }
      throw new UnsupportedUserType(listType);
    }

    public static boolean isCqlTypeSupported(
        com.datastax.oss.driver.api.core.type.ListType cqlListType) {
      Objects.requireNonNull(cqlListType, "cqlListType must not be null");

      // cannot be frozen
      if (cqlListType.isFrozen()) {
        return false;
      }
      // must be a primitive type value
      return cqlListType.getElementType() instanceof PrimitiveType;
    }

    public static boolean isColumnTypeSupported(ComplexColumnDesc.ListColumnDesc listType) {
      Objects.requireNonNull(listType, "listType must not be null");

      try {
        return isValueTypeSupported(ApiDataTypeDefs.from(listType.valueType()));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }

    public static boolean isValueTypeSupported(ApiDataType valueType) {
      Objects.requireNonNull(valueType, "valueType must not be null");

      return valueType.isPrimitive();
    }
  }

  // ===================================================================================================================
  // SetType
  // ===================================================================================================================

  public static class ApiSetType extends ComplexApiDataType {

    public ApiSetType(PrimitiveApiDataTypeDef valueType) {
      super(
          ApiDataTypeName.SET,
          valueType,
          DataTypes.setOf(valueType.getCqlType()),
          new ComplexColumnDesc.SetColumnDesc(valueType.getColumnType()));

      // sanity checking
      if (!isValueTypeSupported(valueType)) {
        throw new IllegalArgumentException("valueType is not supported");
      }
    }

    public static ApiSetType from(ApiDataType valueType) {
      Objects.requireNonNull(valueType, "valueType must not be null");

      if (valueType instanceof PrimitiveApiDataTypeDef vtp) {
        return new ApiSetType(vtp);
      }
      throw new IllegalArgumentException(
          "valueType must be primitive type, valueType%s".formatted(valueType));
    }

    public static ApiSetType from(ComplexColumnDesc.SetColumnDesc setType)
        throws UnsupportedUserType {
      Objects.requireNonNull(setType, "setType must not be null");

      var valueType = ApiDataTypeDefs.from(setType.valueType());
      if (isValueTypeSupported(valueType)) {
        return new ApiSetType((PrimitiveApiDataTypeDef) valueType);
      }
      throw new UnsupportedUserType(setType);
    }

    public static boolean isCqlTypeSupported(
        com.datastax.oss.driver.api.core.type.SetType cqlSetType) {
      Objects.requireNonNull(cqlSetType, "cqlSetType must not be null");

      // cannot be frozen
      if (cqlSetType.isFrozen()) {
        return false;
      }
      // must be a primitive type value
      return cqlSetType.getElementType() instanceof PrimitiveType;
    }

    public static boolean isColumnTypeSupported(ComplexColumnDesc.SetColumnDesc setType) {
      Objects.requireNonNull(setType, "setType must not be null");

      try {
        return isValueTypeSupported(ApiDataTypeDefs.from(setType.valueType()));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }

    public static boolean isValueTypeSupported(ApiDataType valueType) {
      Objects.requireNonNull(valueType, "valueType must not be null");

      return valueType.isPrimitive();
    }
  }

  // ===================================================================================================================
  // VectorType
  // ===================================================================================================================

  public static class ApiVectorType extends ComplexApiDataType {

    private final int dimension;

    public ApiVectorType(
        PrimitiveApiDataTypeDef valueType,
        int dimensions,
        VectorizeDefinition vectorizeDefinition) {
      super(
          ApiDataTypeName.VECTOR,
          valueType,
          new ExtendedVectorType(valueType.getCqlType(), dimensions),
          new ComplexColumnDesc.VectorColumnDesc(
              valueType.getColumnType(),
              dimensions,
              vectorizeDefinition == null ? null : vectorizeDefinition.toVectorizeConfig()));

      this.dimension = dimensions;
      // sanity checking
      if (!isValueTypeSupported(valueType)) {
        throw new IllegalArgumentException("valueType is not supported");
      }
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

    public static ApiSetType from(ComplexColumnDesc.VectorColumnDesc vectorType)
        throws UnsupportedUserType {
      Objects.requireNonNull(vectorType, "vectorType must not be null");

      var valueType = ApiDataTypeDefs.from(vectorType.valueType());
      if (isValueTypeSupported(valueType)) {
        return new ApiSetType((PrimitiveApiDataTypeDef) valueType);
      }
      throw new UnsupportedUserType(vectorType);
    }

    public static boolean isCqlTypeSupported(
        com.datastax.oss.driver.api.core.type.VectorType cqlVectorType) {
      Objects.requireNonNull(cqlVectorType, "cqlVectorType must not be null");

      // Must be a float
      return cqlVectorType.getElementType() == DataTypes.FLOAT;
    }

    public static boolean isColumnTypeSupported(ComplexColumnDesc.VectorColumnDesc vectorType) {
      Objects.requireNonNull(vectorType, "vectorType must not be null");

      try {
        return isValueTypeSupported(ApiDataTypeDefs.from(vectorType.valueType()));
      } catch (UnsupportedUserType e) {
        return false;
      }
    }

    public static boolean isValueTypeSupported(ApiDataType valueType) {
      Objects.requireNonNull(valueType, "valueType must not be null");

      return valueType == ApiDataTypeDefs.FLOAT;
    }
  }
}
