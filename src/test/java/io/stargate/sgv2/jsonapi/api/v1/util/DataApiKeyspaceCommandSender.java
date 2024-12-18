package io.stargate.sgv2.jsonapi.api.v1.util;

import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.KeyspaceResource;

public class DataApiKeyspaceCommandSender
    extends DataApiCommandSenderBase<DataApiKeyspaceCommandSender> {

  private KeyspaceTemplates keyspaceTemplates;

  protected DataApiKeyspaceCommandSender(String keyspace) {
    super(keyspace);
    this.keyspaceTemplates = new KeyspaceTemplates(this);
  }

  public KeyspaceTemplates templated() {
    return keyspaceTemplates;
  }

  protected io.restassured.response.Response postInternal(RequestSpecification request) {
    return request.post(KeyspaceResource.BASE_PATH, keyspace);
  }

  // ===================================================================================================================
  // DDL - TABLES
  // ===================================================================================================================

  public DataApiResponseValidator postCreateTable(String jsonClause) {
    return postCommand(CommandName.CREATE_TABLE, jsonClause);
  }

  public DataApiResponseValidator postDropTable(String jsonClause) {
    return postCommand(CommandName.DROP_TABLE, jsonClause);
  }

  public DataApiResponseValidator postDropIndex(String jsonClause) {
    return postCommand(CommandName.DROP_INDEX, jsonClause);
  }

  public DataApiResponseValidator postListTables(String jsonClause) {
    return postCommand(CommandName.LIST_TABLES, jsonClause);
  }

  // ===================================================================================================================
  // DDL - COLLECTIONS
  // ===================================================================================================================

  public DataApiResponseValidator postCreateCollection(String jsonClause) {
    return postCommand(CommandName.CREATE_COLLECTION, jsonClause);
  }
}
