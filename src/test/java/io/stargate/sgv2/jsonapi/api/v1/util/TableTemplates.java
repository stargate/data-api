package io.stargate.sgv2.jsonapi.api.v1.util;

public class TableTemplates extends TemplateRunner {

  private DataApiTableCommandSender sender;

  public TableTemplates(DataApiTableCommandSender sender) {
    this.sender = sender;
  }

  // ==================================================================================================================
  // DML - INSERT / DELETE / UPDATE
  // ==================================================================================================================

  public DataApiResponseValidator insertOne(String document) {
    var json =
            """
             {
              "document": %s
             }
        """
            .formatted(document);
    return sender.postInsertOne(json);
  }

  public DataApiResponseValidator insertMany(String... documents) {
    var json =
            """
             {
              "documents": [%s]
             }
        """
            .formatted(String.join(",", documents));
    return sender.postInsertMany(json);
  }

  // ==================================================================================================================
  // DDL - TABLE / INDEX
  // ==================================================================================================================

  public DataApiResponseValidator createIndex(String indexName, String column) {
    var json =
            """
              {
                "name": "%s",
                "definition": {"column": "%s"}
              }
        """
            .formatted(indexName, column);
    return sender.postCreateIndex(json);
  }

  public DataApiResponseValidator dropIndex(String indexName) {
    var json =
            """
            {
              "indexName": "%s"
            }
          """
            .formatted(indexName);
    return sender.postDropIndex(json);
  }
}
