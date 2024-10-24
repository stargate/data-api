package io.stargate.sgv2.jsonapi.api.v1.util;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import java.util.List;

public class TableTemplates extends TemplateRunner {

  private DataApiTableCommandSender sender;

  public TableTemplates(DataApiTableCommandSender sender) {
    this.sender = sender;
  }

  // ==================================================================================================================
  // DML - INSERT / DELETE / UPDATE / FIND
  // ==================================================================================================================

  public DataApiResponseValidator find(String filter) {
    var json =
            """
             {
              "filter": %s
             }
          """
            .formatted(filter);
    return sender.postFind(json);
  }

  public DataApiResponseValidator updateOne(String filter, String update) {
    var json =
            """
             {
              "filter": %s ,
              "update": %s
             }
          """
            .formatted(filter, update);
    return sender.postUpdateOne(json);
  }

  public DataApiResponseValidator deleteMany(String filter) {
    var json =
            """
               {
                "filter": %s
               }
            """
            .formatted(filter);
    return sender.postDeleteMany(json);
  }

  public DataApiResponseValidator deleteOne(String filter) {
    var json =
            """
               {
                "filter": %s
               }
            """
            .formatted(filter);
    return sender.postDeleteOne(json);
  }

  public DataApiResponseValidator delete(Command.CommandName deleteCommand, String filterJSON) {
    return switch (deleteCommand) {
      case DELETE_ONE -> deleteOne(filterJSON);
      case DELETE_MANY -> deleteMany(filterJSON);
      default ->
          throw new IllegalArgumentException("Unexpected command for delete: " + deleteCommand);
    };
  }

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
    return insertMany(List.of(documents));
  }

  public DataApiResponseValidator insertMany(List<String> documents) {
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

  public DataApiResponseValidator alterTable(String alterOperation, Object columns) {
    var json =
            """
            {
              "operation" : {
                "%s": {
                  "columns" : %s
                }
              }
            }
          """
            .formatted(alterOperation, asJSON(columns));
    return sender.postAlterTable(json);
  }
}
