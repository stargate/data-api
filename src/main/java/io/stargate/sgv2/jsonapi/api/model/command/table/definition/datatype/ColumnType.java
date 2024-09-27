package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDefinitionDeserializer;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.util.Map;

@JsonDeserialize(using = ColumnDefinitionDeserializer.class)
/** Interface for column types. This is used to define the type of a column in a table. */
public interface ColumnType {
  // Returns cassandra data type.
  ApiDataType getApiDataType();

  // Returns the column type from the string.
  static ColumnType fromString(String type) {
    // TODO: the name of the type should be a part of the ColumnType interface, and use a map for
    // the lookup
    switch (type) {
      case "ascii":
        return PrimitiveTypes.ASCII;
      case "bigint":
        return PrimitiveTypes.BIGINT;
      case "blob":
        return PrimitiveTypes.BINARY;
      case "boolean":
        return PrimitiveTypes.BOOLEAN;
      case "decimal":
        return PrimitiveTypes.DECIMAL;
      case "double":
        return PrimitiveTypes.DOUBLE;
      case "float":
        return PrimitiveTypes.FLOAT;
      case "int":
        return PrimitiveTypes.INT;
      case "smallint":
        return PrimitiveTypes.SMALLINT;
      case "text":
        return PrimitiveTypes.TEXT;
      case "tinyint":
        return PrimitiveTypes.TINYINT;
      case "varint":
        return PrimitiveTypes.VARINT;

      default:
        {
          Map<String, String> errorMessageFormattingValues =
              Map.of(
                  "type",
                  type,
                  "supported_types",
                  "[ascii, bigint, blob, boolean, decimal, double, float, int, smallint, text, tinyint, varint]");
          throw SchemaException.Code.COLUMN_TYPE_UNSUPPORTED.get(errorMessageFormattingValues);
        }
    }
  }
}
