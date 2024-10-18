package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;

public class DataApiTableCommandSender extends DataApiCommandSenderBase<DataApiTableCommandSender> {
  private final String tableName;

  protected DataApiTableCommandSender(String keyspace, String tableName) {
    super(keyspace);
    this.tableName = tableName;
  }

  @Override
  protected io.restassured.response.Response postInternal(RequestSpecification request) {
    return request.post(CollectionResource.BASE_PATH, keyspace, tableName);
  }

  public DataApiResponseValidator postDeleteMany(String deleteManyClause) {
    return postCommand("deleteMany", deleteManyClause);
  }

  /**
   * Partially typed method for sending a POST command to the Data API: caller is responsible for
   * formatting the clause to include as (JSON Object) argument of "finOne" command.
   *
   * @param findOneClause JSON clause to include in the "findOne" command: minimally empty JSON
   *     Object ({@code { } })
   * @return Response validator for further assertions
   */
  public DataApiResponseValidator postFindOne(String findOneClause) {
    return postCommand("findOne", findOneClause);
  }

  public DataApiResponseValidator postInsertOne(String docAsJSON) {
    return postCommand("insertOne", "{ \"document\": %s }".formatted(docAsJSON));
  }

  public DataApiResponseValidator postCreateIndex(String columnName, String indexName) {
    String createIndex =
        "{\"name\": \"%s\", \"definition\": { \"column\": \"%s\"} }"
            .formatted(indexName, columnName);
    return postCommand("createIndex", createIndex);
  }

  public DataApiResponseValidator postAlterTable(String tableDefAsJSON) {
    return postCommand("alterName", tableDefAsJSON);
  }
}
