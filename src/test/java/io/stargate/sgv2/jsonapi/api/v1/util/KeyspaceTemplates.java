package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiClusteringDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KeyspaceTemplates extends TemplateRunner {

  private DataApiKeyspaceCommandSender sender;

  public KeyspaceTemplates(DataApiKeyspaceCommandSender sender) {
    this.sender = sender;
  }

  // ===================================================================================================================
  // DDL - TABLES
  // ===================================================================================================================

  public DataApiResponseValidator createTable(
      String tableName, ApiColumnDefContainer columns, ApiColumnDef primaryKeyDef) {
    var json =
            """
        {
            "name": "%s",
            "definition": {
                "columns": %s,
                "primaryKey": %s
            }
        }
        """
            .formatted(
                tableName,
                columnsAsJSON(columns.getSchemaDescription(SchemaDescSource.DDL_USAGE)),
                asJSON(primaryKeyDef.name().asInternal()));
    return sender.postCreateTable(json);
  }

  public DataApiResponseValidator createTable(
      String tableName,
      ApiColumnDefContainer columns,
      ApiColumnDefContainer primaryKeys,
      List<ApiClusteringDef> clusteringDefs) {

    var primaryKey = new LinkedHashMap<String, Object>();
    primaryKey.put(
        "partitionBy",
        primaryKeys.values().stream().map(def -> cqlIdentifierToJsonKey(def.name())).toList());

    var partitionSort = new LinkedHashMap<String, Object>();
    clusteringDefs.forEach(
        def ->
            partitionSort.put(
                cqlIdentifierToJsonKey(def.columnDef().name()),
                def.order().getOrderDesc().ordinal));
    primaryKey.put("partitionSort", partitionSort);

    var json =
            """
          {
              "name": "%s",
              "definition": {
                  "columns": %s,
                  "primaryKey": %s
              }
          }
          """
            .formatted(
                tableName,
                columnsAsJSON(columns.getSchemaDescription(SchemaDescSource.DDL_USAGE)),
                asJSON(primaryKey));
    return sender.postCreateTable(json);
  }

  public DataApiResponseValidator createTable(
      String tableName, Map<String, Object> columns, Object primaryKeyDef) {
    var json =
            """
            {
                "name": "%s",
                "definition": {
                    "columns": %s,
                    "primaryKey": %s
                }
            }
            """
            .formatted(tableName, asJSON(columns), asJSON(primaryKeyDef));
    return sender.postCreateTable(json);
  }

  private String columnsAsJSON(ColumnsDescContainer columnDefs) {
    // Unfortunately column defs includes invalid "apiSupport" property in some cases;
    // need to filter out
    JsonNode columnDefsNode = asJsonNode(columnDefs);
    for (Map.Entry<String, JsonNode> entry : columnDefsNode.properties()) {
      Iterator<Map.Entry<String, JsonNode>> it = entry.getValue().properties().iterator();
      while (it.hasNext()) {
        if ("apiSupport".equals(it.next().getKey())) {
          it.remove();
        }
      }
    }
    return asJSON(columnDefsNode);
  }

  public DataApiResponseValidator createType(String typeName, Map<String, Object> fields) {
    var json =
            """
            {
                "name": "%s",
                "definition": {
                    "fields": %s
                }
            }
            """
            .formatted(typeName, asJSON(fields));
    return sender.postCreateType(json);
  }

  public DataApiResponseValidator createType(String typeName, String fields) {
    var json =
            """
        {
            "name": "%s",
            "definition": {
                "fields": %s
            }
        }
        """
            .formatted(typeName, fields);
    return sender.postCreateType(json);
  }

  public DataApiResponseValidator dropIndex(String indexName, boolean ifExists) {
    String json =
            """
            {
              "name": "%s",
                "options": {
                    "ifExists": %s
                }
            }
        """
            .formatted(indexName, String.valueOf(ifExists));
    return sender.postDropIndex(json);
  }

  public DataApiResponseValidator dropTable(String tableName, boolean ifExists) {
    String json =
            """
            {
              "name": "%s",
              "options": {
                "ifExists": %s
              }
            }
        """
            .formatted(tableName, String.valueOf(ifExists));
    return sender.postDropTable(json);
  }

  public DataApiResponseValidator dropType(String typeName, boolean ifExists) {
    String json =
            """
            {
              "name": "%s",
              "options": {
                "ifExists": %s
              }
            }
        """
            .formatted(typeName, String.valueOf(ifExists));
    return sender.postDropType(json);
  }

  public DataApiResponseValidator alterType(
      String typeName, Map<String, Object> addingFields, Map<String, String> renamingFields) {
    var json =
            """
            {
                "name": "%s",
                "add": {
                    "fields": %s
                },
                "rename": {
                    "fields": %s
                }
            }
            """
            .formatted(typeName, asJSON(addingFields), asJSON(renamingFields));
    return sender.postAlterType(json);
  }

  public DataApiResponseValidator alterType(String typeName, String alterOps) {
    var json =
            """
        {
            "name": "%s",
            %s
        }
        """
            .formatted(typeName, alterOps);
    return sender.postAlterType(json);
  }

  public DataApiResponseValidator listTypes(boolean explain) {
    String json =
            """
            {
            "options" : {
                "explain" : %s
            }
         }
         """
            .formatted(explain);
    return sender.postListTypes(json);
  }

  public DataApiResponseValidator listTables(boolean explain) {
    String json =
            """
        {
          "options" : {
            "explain" : %s
          }
       }
        """
            .formatted(explain);
    return sender.postListTables(json);
  }

  // ===================================================================================================================
  // DDL - COLLECTIONS
  // ===================================================================================================================

  public DataApiResponseValidator createCollection(String collectionName) {
    var json =
            """
        {
          "name": "%s"
        }
        """
            .formatted(collectionName);
    return sender.postCreateCollection(json);
  }
}
