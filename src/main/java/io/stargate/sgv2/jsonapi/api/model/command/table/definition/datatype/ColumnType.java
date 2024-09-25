package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;

/** Interface for column types. This is used to define the type of a column in a table. */
public interface ColumnType {
  // Returns cassandra data type.
  DataType getCqlType();

  // Returns the column type from the string.
  static ColumnType fromString(String type) {
    // TODO: the name of the type should be a part of the ColumnType interface, and use a map for
    // the lookup
    switch (type) {
      case "text":
        return PrimitiveTypes.TEXT;
      case "int":
        return PrimitiveTypes.INT;
      case "boolean":
        return PrimitiveTypes.BOOLEAN;
      case "bigint":
        return PrimitiveTypes.BIGINT;
      case "date":
        return PrimitiveTypes.DATE;
      case "duration":
        return PrimitiveTypes.DURATION;
      case "decimal":
        return PrimitiveTypes.DECIMAL;
      case "double":
        return PrimitiveTypes.DOUBLE;
      case "float":
        return PrimitiveTypes.FLOAT;
      case "smallint":
        return PrimitiveTypes.SMALLINT;
      case "time":
        return PrimitiveTypes.TIME;
      case "timestamp":
        return PrimitiveTypes.TIMESTAMP;
      case "tinyint":
        return PrimitiveTypes.TINYINT;
      case "varint":
        return PrimitiveTypes.VARINT;
      case "ascii":
        return PrimitiveTypes.ASCII;
      case "blob":
        return PrimitiveTypes.BLOB;
      default:
        throw ErrorCodeV1.TABLE_COLUMN_TYPE_UNSUPPORTED.toApiException(
            "Invalid column type: " + type);
    }
  }
}
