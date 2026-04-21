package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.GeneralResource;

public class DataApiGeneralCommandSender
    extends DataApiCommandSenderBase<DataApiGeneralCommandSender> {

  public DataApiResponseValidator postFindEmbeddingProviders() {
    return postCommand(CommandName.FIND_EMBEDDING_PROVIDERS, "{}");
  }

  public DataApiResponseValidator postFindRerankingProviders() {
    return postCommand(CommandName.FIND_RERANKING_PROVIDERS, "{}");
  }

  protected io.restassured.response.Response postInternal(RequestSpecification request) {
    return request.post(GeneralResource.BASE_PATH);
  }
}
