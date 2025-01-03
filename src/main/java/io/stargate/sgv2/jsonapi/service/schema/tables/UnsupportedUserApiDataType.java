package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
import java.util.Objects;

/**
 * A data type that we got from a User request but we do not support it (e.g. map with non text
 * key).
 */
public class UnsupportedUserApiDataType extends UnsupportedApiDataType {

  private final ColumnDesc columnDesc;

  UnsupportedUserApiDataType(ColumnDesc columnDesc) {
    this.columnDesc = Objects.requireNonNull(columnDesc, "columnDesc must not be null");
  }

  @Override
  public DataType cqlType() {
    throw new UnsupportedOperationException("UnsupportedUserApiDataType does not have getCqlType");
  }

  @Override
  public ColumnDesc columnDesc() {
    return columnDesc;
  }
}
