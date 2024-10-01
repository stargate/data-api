package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDefinitionDeserializer;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.util.List;
import java.util.Map;

@JsonDeserialize(using = ColumnDefinitionDeserializer.class)

/** Interface for column types. This is used to define the type of a column in a table. */
public interface ColumnType {
  // Returns api data type.
  ApiDataType getApiDataType();

  static List<String> getSupportedTypes() {
    return List.of(
        "ascii",
        "bigint",
        "blob",
        "boolean",
        "date",
        "decimal",
        "double",
        "duration",
        "float",
        "int",
        "list",
        "map",
        "set",
        "smallint",
        "text",
        "time",
        "timestamp",
        "tinyint",
        "varint",
        "vector");
  }

  // Returns the column type from the string.
  static ColumnType fromString(String type, String keyType, String valueType, int dimension) {
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
      case "date":
        return PrimitiveTypes.DATE;
      case "decimal":
        return PrimitiveTypes.DECIMAL;
      case "double":
        return PrimitiveTypes.DOUBLE;
      case "duration":
        return PrimitiveTypes.DURATION;
      case "float":
        return PrimitiveTypes.FLOAT;
      case "int":
        return PrimitiveTypes.INT;
      case "smallint":
        return PrimitiveTypes.SMALLINT;
      case "text":
        return PrimitiveTypes.TEXT;
      case "time":
        return PrimitiveTypes.TIME;
      case "timestamp":
        return PrimitiveTypes.TIMESTAMP;
      case "tinyint":
        return PrimitiveTypes.TINYINT;
      case "varint":
        return PrimitiveTypes.VARINT;
      case "map":
        {
          if (keyType == null || valueType == null) {
            throw SchemaException.Code.MAP_TYPE_INCORRECT_DEFINITION.get();
          }
          try {
            return new ComplexTypes.MapType(
                fromString(keyType, null, null, dimension),
                fromString(valueType, null, null, dimension));
          } catch (SchemaException se) {
            throw SchemaException.Code.MAP_TYPE_INCORRECT_DEFINITION.get();
          }
        }
      case "list":
        {
          if (valueType == null) {
            throw SchemaException.Code.LIST_TYPE_INCORRECT_DEFINITION.get();
          }
          try {
            return new ComplexTypes.ListType(fromString(valueType, null, null, dimension));
          } catch (SchemaException se) {
            throw SchemaException.Code.LIST_TYPE_INCORRECT_DEFINITION.get();
          }
        }

      case "set":
        {
          if (valueType == null) {
            throw SchemaException.Code.SET_TYPE_INCORRECT_DEFINITION.get();
          }
          try {
            return new ComplexTypes.SetType(fromString(valueType, null, null, dimension));
          } catch (SchemaException se) {
            throw SchemaException.Code.SET_TYPE_INCORRECT_DEFINITION.get();
          }
        }

      case "vector":
        {
          if (valueType == null) {
            valueType = "float";
          }
          if (dimension <= 0) {
            throw SchemaException.Code.VECTOR_TYPE_INCORRECT_DEFINITION.get();
          }
          try {
            return new ComplexTypes.VectorType(
                fromString(valueType, null, null, dimension), dimension);
          } catch (SchemaException se) {
            throw SchemaException.Code.VECTOR_TYPE_INCORRECT_DEFINITION.get();
          }
        }
      default:
        {
          Map<String, String> errorMessageFormattingValues =
              Map.of(
                  "type",
                  type,
                  "supported_types",
                  "[" + String.join(", ", ColumnType.getSupportedTypes()) + "]");
          throw SchemaException.Code.COLUMN_TYPE_UNSUPPORTED.get(errorMessageFormattingValues);
        }
    }
  }
}
