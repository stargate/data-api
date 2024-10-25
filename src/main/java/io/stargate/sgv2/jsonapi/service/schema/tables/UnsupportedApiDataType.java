package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnDesc;
import java.util.Objects;

public class UnsupportedApiDataType implements ApiDataType {

  private final DataType cqlType;
  private final ColumnDesc columnDesc;

  public UnsupportedApiDataType(DataType cqlType) {
    this.cqlType = Objects.requireNonNull(cqlType, "cqlType must not be null");
    this.columnDesc = new ComplexColumnDesc.UnsupportedType(cqlType);
  }

  @Override
  public ApiDataTypeName getName() {
    throw new UnsupportedOperationException("UnsupportedApiDataType does not have a name");
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public boolean isContainer() {
    return false;
  }

  @Override
  public boolean isUnsupported() {
    return true;
  }

  @Override
  public ColumnDesc getColumnType() {
    return columnDesc;
  }
}
