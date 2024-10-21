package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.v1.CollectionResource;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;

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

  public DataApiResponseValidator postFindOneByFilter(String filterAsJSON) {
    return postCommand("findOne", "{ \"filter\": %s }".formatted(filterAsJSON));
  }

  public DataApiResponseValidator postFind(String findClause) {
    return postCommand("find", findClause);
  }

  public DataApiResponseValidator postFindByFilter(String filterAsJSON) {
    return postCommand("find", "{ \"filter\": %s }".formatted(filterAsJSON));
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

  public DataApiResponseValidator postDelete(
      WhereCQLClauseAnalyzer.StatementType statementType, String filterAsJSON) {
    if (statementType == WhereCQLClauseAnalyzer.StatementType.DELETE_ONE) {
      return postCommand("deleteOne", "{ \"filter\": %s }".formatted(filterAsJSON));
    }
    if (statementType == WhereCQLClauseAnalyzer.StatementType.DELETE_MANY) {
      return postCommand("deleteMany", "{ \"filter\": %s }".formatted(filterAsJSON));
    }
    throw new IllegalArgumentException("Invalid statementType: " + statementType.name());
  }

  public DataApiResponseValidator postUpdateOne(String filterAsJSON, String updateClause) {
    return postCommand(
        "updateOne", "{ \"filter\": %s , \"update\": %s}".formatted(filterAsJSON, updateClause));
  }

  // Table does not support updateMany command
  public DataApiResponseValidator postUpdateMany(String filterAsJSON, String updateClause) {
    return postCommand(
        "updateMany", "{ \"filter\": %s , \"update\": %s}".formatted(filterAsJSON, updateClause));
  }
}
