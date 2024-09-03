package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.v1.NamespaceResource;

public class DataApiNamespaceCommandSender
    extends DataApiCommandSenderBase<DataApiNamespaceCommandSender> {
  protected DataApiNamespaceCommandSender(String namespace) {
    super(namespace);
  }

  protected io.restassured.response.Response postInternal(RequestSpecification request) {
    return request.post(NamespaceResource.BASE_PATH, namespace);
  }

  public DataApiResponseValidator postCreateTable(String tableDefAsJSON) {
    return postCommand("createTable", tableDefAsJSON);
  }
}
