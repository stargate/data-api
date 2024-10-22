package io.stargate.sgv2.jsonapi.api.v1.util;

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
