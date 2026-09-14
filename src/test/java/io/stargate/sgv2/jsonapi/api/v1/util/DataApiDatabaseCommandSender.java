package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;

public class DataApiDatabaseCommandSender
    extends DataApiCommandSenderBase<DataApiDatabaseCommandSender> {

  private final DatabaseTemplates templates;

  public DataApiDatabaseCommandSender() {
    this.templates = new DatabaseTemplates(this);
  }

  public DatabaseTemplates templated() {
    return this.templates;
  }

  public DataApiResponseValidator postFindEmbeddingProviders() {
    return postCommand(CommandName.FIND_EMBEDDING_PROVIDERS, "{}");
  }

  public DataApiResponseValidator postFindRerankingProviders() {
    return postCommand(CommandName.FIND_RERANKING_PROVIDERS, "{}");
  }

  public DataApiResponseValidator postCreateKeyspace(String jsonClause) {
    return postCommand(CommandName.CREATE_KEYSPACE, jsonClause);
  }

  public DataApiResponseValidator postDropKeyspace(String jsonClause) {
    return postCommand(CommandName.DROP_KEYSPACE, jsonClause);
  }

  protected io.restassured.response.Response postInternal(RequestSpecification request) {
    return request.post(GeneralResource.BASE_PATH);
  }
}
