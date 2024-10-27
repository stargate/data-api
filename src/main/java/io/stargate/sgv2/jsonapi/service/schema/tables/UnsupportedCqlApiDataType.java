package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.UnsupportedColumnDesc;
import java.util.Objects;

public class UnsupportedCqlApiDataType extends UnsupportedApiDataType {

  private final DataType cqlType;
  private final ColumnDesc columnDesc;

  UnsupportedCqlApiDataType(DataType cqlType) {
    this.cqlType = Objects.requireNonNull(cqlType, "cqlType must not be null");
    this.columnDesc = new UnsupportedColumnDesc.UnsupportedCqlColumnDesc(cqlType);
  }

  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public ColumnDesc getColumnDesc() {
    return columnDesc;
  }
}
