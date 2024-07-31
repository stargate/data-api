package io.stargate.sgv2.jsonapi.api.model.command.column.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;

public interface ColumnType {
  // Returns cassandra data type.
  DataType getCqlType();

  static ColumnType fromString(String type) {
    switch (type) {
      case "text":
        return PrimitiveTypes.TEXT;
      case "int":
        return PrimitiveTypes.INT;
      default:
        throw ErrorCode.UNSUPPORTED_COLUMN_TYPE.toApiException("Invalid column type: " + type);
    }
  }
}
