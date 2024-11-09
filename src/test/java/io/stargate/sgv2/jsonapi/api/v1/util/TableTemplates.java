package io.stargate.sgv2.jsonapi.api.v1.util;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableTemplates extends TemplateRunner {

  private DataApiTableCommandSender sender;

  public TableTemplates(DataApiTableCommandSender sender) {
    this.sender = sender;
  }

  // ==================================================================================================================
  // DML - FIND
  // ==================================================================================================================

  private String findClause(
      Map<String, Object> filter,
      List<String> projection,
      Map<String, Object> sort,
      Map<String, Object> options) {

    var clause = new LinkedHashMap<>();
    if (filter != null) {
      clause.put("filter", filter);
    }
    if (projection != null) {
      // this is for the positive projection of selecting columns
      clause.put("projection", projection.stream().collect(Collectors.toMap(col -> col, col -> 1)));
    }
    if (sort != null) {
      clause.put("sort", sort);
    }
    if (options != null) {
      clause.put("options", options);
    }
    return asJSON(clause);
  }

  public DataApiResponseValidator find(
      Command.CommandName commandName, Map<String, Object> filter, List<String> columns) {
    return find(commandName, filter, columns, null);
  }

  public DataApiResponseValidator find(
      Command.CommandName commandName,
      Map<String, Object> filter,
      List<String> columns,
      Map<String, Object> sort) {
    return find(commandName, filter, columns, sort, null);
  }

  public DataApiResponseValidator find(
      Command.CommandName commandName,
      Map<String, Object> filter,
      List<String> columns,
      Map<String, Object> sort,
      Map<String, Object> options) {
    return switch (commandName) {
      case FIND_ONE -> findOne(filter, columns, sort, options);
      case FIND -> find(filter, columns, sort, options);
      default -> throw new IllegalArgumentException("Unexpected command for find: " + commandName);
    };
  }

  public DataApiResponseValidator findOne(Map<String, Object> filter, List<String> columns) {
    return findOne(filter, columns, null, null);
  }

  public DataApiResponseValidator findOne(
      Map<String, Object> filter, List<String> columns, Map<String, Object> sort) {
    return findOne(filter, columns, sort, null);
  }

  public DataApiResponseValidator findOne(
      Map<String, Object> filter,
      List<String> columns,
      Map<String, Object> sort,
      Map<String, Object> options) {
    return sender.postFindOne(findClause(filter, columns, sort, options));
  }

  public DataApiResponseValidator find(Map<String, Object> filter, List<String> projection) {
    return find(filter, projection, null);
  }

  public DataApiResponseValidator find(
      Map<String, Object> filter, List<String> projection, Map<String, Object> sort) {
    return find(filter, projection, sort, null);
  }

  public DataApiResponseValidator find(
      Map<String, Object> filter,
      List<String> projection,
      Map<String, Object> sort,
      Map<String, Object> options) {
    return sender.postFind(findClause(filter, projection, sort, options));
  }

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

  // ==================================================================================================================
  // DML - INSERT / DELETE / UPDATE
  // ==================================================================================================================

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

  public DataApiResponseValidator updateOne(
      Map<String, Object> filter, Map<String, Object> update) {
    return sender.postUpdateOne(updateClause(filter, update));
  }

  private String updateClause(Map<String, Object> filter, Map<String, Object> update) {
    var clause = new LinkedHashMap<>();
    if (filter != null) {
      clause.put("filter", filter);
    }
    if (update != null) {
      clause.put("update", update);
    }
    return asJSON(clause);
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

  public DataApiResponseValidator insertManyMap(List<Map<String, Object>> documents) {
    return insertMany(documents.stream().map(TemplateRunner::asJSON).collect(Collectors.toList()));
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

  public DataApiResponseValidator createVectorIndex(String indexName, String column) {
    var json =
            """
          {
            "name": "%s",
            "definition": {"column": "%s"}
          }
        """
            .formatted(indexName, column);
    return sender.postCreateVectorIndex(json);
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

  public DataApiResponseValidator listIndexes(boolean explain) {
    String json =
            """
        {
          "options" : {
            "explain" : %s
          }
       }
        """
            .formatted(explain);
    return sender.postListIndexes(json);
  }
}
