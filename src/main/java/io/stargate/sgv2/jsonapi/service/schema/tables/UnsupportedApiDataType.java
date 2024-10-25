package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ComplexColumnType;
import java.util.Objects;

public class UnsupportedApiDataType implements ApiDataType {

  private final DataType cqlType;
  private final ColumnType columnType;

  public UnsupportedApiDataType(DataType cqlType) {
    this.cqlType = Objects.requireNonNull(cqlType, "cqlType must not be null");
    this.columnType = new ComplexColumnType.UnsupportedType(cqlType);
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
  public ColumnType getColumnType() {
    return columnType;
  }
}
