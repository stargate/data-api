package io.stargate.sgv2.jsonapi.api.v1.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableTemplates extends TemplateRunner {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private DataApiTableCommandSender sender;

  public TableTemplates(DataApiTableCommandSender sender) {
    this.sender = sender;
  }

  // ==================================================================================================================
  // DML - INSERT / DELETE / UPDATE
  // ==================================================================================================================

  private String findClause(Map<String, Object> filter, List<String> columns) {
    var projection = columns.stream().collect(Collectors.toMap(col -> col, col -> 1));

    var clause =
        Map.of(
            "filter", filter,
            "projection", projection);

    try {
      return MAPPER.writeValueAsString(clause);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public DataApiResponseValidator find(
      Command.CommandName commandName, Map<String, Object> filter, List<String> columns) {
    return switch (commandName) {
      case FIND_ONE -> findOne(filter, columns);
      case FIND -> find(filter, columns);
      default -> throw new IllegalArgumentException("Unexpected command for find: " + commandName);
    };
  }

  public DataApiResponseValidator findOne(Map<String, Object> filter, List<String> columns) {
    return sender.postFindOne(findClause(filter, columns));
  }

  public DataApiResponseValidator find(Map<String, Object> filter, List<String> columns) {
    return sender.postFind(findClause(filter, columns));
  }

  // ==================================================================================================================
  // DML - INSERT / DELETE / UPDATE
  // ==================================================================================================================

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
