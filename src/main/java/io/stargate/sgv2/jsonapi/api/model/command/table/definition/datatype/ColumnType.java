package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDefinitionDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDefinitionSerializer;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import java.util.List;
import java.util.Map;

/** Interface for column types. This is used to define the type of a column in a table. */
@JsonDeserialize(using = ColumnDefinitionDeserializer.class)
@JsonSerialize(using = ColumnDefinitionSerializer.class)
public interface ColumnType {
  // Returns api data type.
  ApiDataType getApiDataType();

  public String name();

  default String getApiName() {
    return getApiDataType().getApiName();
  }

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
        "inet",
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
        "uuid",
        "vector");
  }

  // Returns the column type from the string.
  static ColumnType fromString(
      String type, String keyType, String valueType, int dimension, VectorizeConfig vectorConfig) {
    // TODO: the name of the type should be a part of the ColumnType interface, and use a map for
    // the lookup
    switch (type) {
      case "map":
        {
          if (keyType == null || valueType == null) {
            throw SchemaException.Code.MAP_TYPE_INCORRECT_DEFINITION.get();
          }
          try {
            return new ComplexTypes.MapType(
                fromString(keyType, null, null, dimension, vectorConfig),
                fromString(valueType, null, null, dimension, vectorConfig));
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
            return new ComplexTypes.ListType(
                fromString(valueType, null, null, dimension, vectorConfig));
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
            return new ComplexTypes.SetType(
                fromString(valueType, null, null, dimension, vectorConfig));
          } catch (SchemaException se) {
            throw SchemaException.Code.SET_TYPE_INCORRECT_DEFINITION.get();
          }
        }

      case "vector":
        {
          if (dimension <= 0) {
            throw SchemaException.Code.VECTOR_TYPE_INCORRECT_DEFINITION.get();
          }
          try {
            return new ComplexTypes.VectorType(PrimitiveTypes.FLOAT, dimension, vectorConfig);
          } catch (SchemaException se) {
            throw SchemaException.Code.VECTOR_TYPE_INCORRECT_DEFINITION.get();
          }
        }
      default:
        {
          ColumnType columnType = PrimitiveTypes.fromString(type);
          if (columnType != null) {
            return columnType;
          }
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
