package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.serializer.ColumnDescSerializer;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeName;
import java.util.List;
import java.util.Map;

/** Interface for column types. This is used to define the type of a column in a table. */
@JsonDeserialize(using = ColumnDescDeserializer.class)
@JsonSerialize(using = ColumnDescSerializer.class)
public interface ColumnDesc {

  ApiDataTypeName typeName();

  /**
   * Gets the string name of the apiType, this is here so that the implementation for unsupported
   * types can return a string and not need to have an unsupported {@link ApiDataTypeName}
   */
  default String getApiName() {
    return typeName().getApiName();
  }

  ApiSupportDesc apiSupport();

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

  /**
   * This is used as part of the deserialization from JSON
   *
   * <p>TODO: it should be in the ColumnDescDeserializer
   */
  static ColumnDesc fromJsonString(
      String type, String keyType, String valueType, int dimension, VectorizeConfig vectorConfig) {
    // TODO: the name of the type should be a part of the ColumnDesc interface, and use a map for
    // the lookup
    switch (type) {
      case "map":
        {
          if (keyType == null || valueType == null) {
            throw SchemaException.Code.MAP_TYPE_INVALID_DEFINITION.get(
                Map.of("reason", "`keyType` or `valueType` is null"));
          }
          final ColumnDesc keyColumnDesc, valueColumnDesc;
          try {
            keyColumnDesc = fromJsonString(keyType, null, null, dimension, vectorConfig);
            valueColumnDesc = fromJsonString(valueType, null, null, dimension, vectorConfig);
          } catch (SchemaException se) {
            throw SchemaException.Code.MAP_TYPE_INVALID_DEFINITION.get(
                Map.of("reason", "Data types used for `keyType` or `valueType` are not supported"));
          }
          if (!(PrimitiveColumnDesc.TEXT.equals(keyColumnDesc)
              || PrimitiveColumnDesc.ASCII.equals(keyColumnDesc))) {
            throw SchemaException.Code.MAP_TYPE_INVALID_DEFINITION.get(
                Map.of("reason", "`keyType` must be `text` or `ascii`, but was " + keyType));
          }
          return new ComplexColumnDesc.MapColumnDesc(keyColumnDesc, valueColumnDesc);
        }
      case "list":
        {
          if (valueType == null) {
            throw SchemaException.Code.LIST_TYPE_INVALID_DEFINITION.get();
          }
          try {
            return new ComplexColumnDesc.ListColumnDesc(
                fromJsonString(valueType, null, null, dimension, vectorConfig));
          } catch (SchemaException se) {
            throw SchemaException.Code.LIST_TYPE_INVALID_DEFINITION.get();
          }
        }

      case "set":
        {
          if (valueType == null) {
            throw SchemaException.Code.SET_TYPE_INVALID_DEFINITION.get();
          }
          try {
            return new ComplexColumnDesc.SetColumnDesc(
                fromJsonString(valueType, null, null, dimension, vectorConfig));
          } catch (SchemaException se) {
            throw SchemaException.Code.SET_TYPE_INVALID_DEFINITION.get();
          }
        }

      case "vector":
        {
          if (dimension <= 0) {
            throw SchemaException.Code.VECTOR_TYPE_INVALID_DEFINITION.get();
          }
          try {
            return new ComplexColumnDesc.VectorColumnDesc(
                PrimitiveColumnDesc.FLOAT, dimension, vectorConfig);
          } catch (SchemaException se) {
            throw SchemaException.Code.VECTOR_TYPE_INVALID_DEFINITION.get();
          }
        }
      default:
        {
          ColumnDesc columnDesc = PrimitiveColumnDesc.fromApiTypeName(type);
          if (columnDesc != null) {
            return columnDesc;
          }
          Map<String, String> errorMessageFormattingValues =
              Map.of(
                  "type",
                  type,
                  "supported_types",
                  "[" + String.join(", ", ColumnDesc.getSupportedTypes()) + "]");
          throw SchemaException.Code.COLUMN_TYPE_UNSUPPORTED.get(errorMessageFormattingValues);
        }
    }
  }
}
