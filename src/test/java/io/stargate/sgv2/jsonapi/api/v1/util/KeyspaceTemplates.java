package io.stargate.sgv2.jsonapi.api.v1.util;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import io.stargate.sgv2.jsonapi.service.schema.tables.ApiClusteringDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
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
                asJSON(columns.toColumnsDesc()),
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
            .formatted(tableName, asJSON(columns.toColumnsDesc()), asJSON(primaryKey));
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
