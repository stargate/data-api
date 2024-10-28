package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;

/** Interface defining the api data type for complex types */
public abstract class CollectionApiDataType implements ApiDataType {
  private final ApiTypeName typeName;
  private final PrimitiveApiDataTypeDef valueType;
  private final DataType cqlType;
  private final ColumnDesc columnDesc;

  protected CollectionApiDataType(
      ApiTypeName typeName,
      PrimitiveApiDataTypeDef valueType,
      DataType cqlType,
      ColumnDesc columnDesc) {
    this.typeName = typeName;
    this.valueType = valueType;
    this.cqlType = cqlType;
    this.columnDesc = columnDesc;
  }

  @Override
  public ApiTypeName typeName() {
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
    return false;
  }

  @Override
  public DataType cqlType() {
    return cqlType;
  }

  @Override
  public ColumnDesc columnDesc() {
    return columnDesc;
  }

  public PrimitiveApiDataTypeDef getValueType() {
    return valueType;
  }
}
