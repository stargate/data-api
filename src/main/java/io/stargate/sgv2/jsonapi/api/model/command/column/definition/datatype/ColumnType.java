package io.stargate.sgv2.jsonapi.api.model.command.column.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;

/** Interface for column types. This is used to define the type of a column in a table. */
public interface ColumnType {
  // Returns cassandra data type.
  DataType getCqlType();

  // Returns the column type from the string.
  static ColumnType fromString(String type) {
    switch (type) {
      case "text":
        return PrimitiveTypes.TEXT;
      case "int":
        return PrimitiveTypes.INT;
      default:
        throw ErrorCode.TABLE_COLUMN_TYPE_UNSUPPORTED.toApiException(
            "Invalid column type: " + type);
    }
  }
}
