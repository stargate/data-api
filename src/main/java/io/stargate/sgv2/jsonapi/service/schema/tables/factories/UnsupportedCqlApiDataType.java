package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescBindingPoint;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.UnsupportedColumnDesc;
import java.util.Objects;

/** A data type that we read from a CQL table and do not support. */
public class UnsupportedCqlApiDataType extends UnsupportedApiDataType {

  private final DataType cqlType;

  UnsupportedCqlApiDataType(DataType cqlType) {
    this.cqlType = Objects.requireNonNull(cqlType, "cqlType must not be null");
  }

  @Override
  public DataType cqlType() {
    return cqlType;
  }

  @Override
  public ColumnDesc getSchemaDescription(SchemaDescBindingPoint bindingPoint) {
    // Always has same representation

    return new UnsupportedColumnDesc.UnsupportedCqlColumnDesc(apiSupport(), cqlType);
  }
}
