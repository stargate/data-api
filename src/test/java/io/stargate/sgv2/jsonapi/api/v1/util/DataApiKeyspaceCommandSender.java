package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource;

public class DataApiKeyspaceCommandSender
    extends DataApiCommandSenderBase<DataApiKeyspaceCommandSender> {
  protected DataApiKeyspaceCommandSender(String keyspace) {
    super(keyspace);
  }

  protected io.restassured.response.Response postInternal(RequestSpecification request) {
    return request.post(KeyspaceResource.BASE_PATH, keyspace);
  }

  public DataApiResponseValidator postCreateTable(String tableDefAsJSON) {
    return postCommand("createTable", tableDefAsJSON);
  }
}
