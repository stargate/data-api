package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import java.util.Arrays;

/** Scope required for an MCP tool in addition to the underlying database permissions. */
enum McpOAuthScope {
  READ("astra:db:read"),
  WRITE("astra:db:write"),
  ADMIN("astra:db:admin");

  private final String value;

  McpOAuthScope(String value) {
    this.value = value;
  }

  String value() {
    return value;
  }

  boolean isGrantedBy(Object claim) {
    if (!(claim instanceof String scope)) {
      return false;
    }
    var granted = Arrays.asList(scope.split(" +"));
    return granted.contains(value) || (this == READ && granted.contains(WRITE.value));
  }

  static McpOAuthScope requiredFor(CommandName command) {
    return switch (command) {
      case COUNT_DOCUMENTS,
              ESTIMATED_DOCUMENT_COUNT,
              FIND,
              FIND_AND_RERANK,
              FIND_COLLECTIONS,
              FIND_EMBEDDING_PROVIDERS,
              FIND_KEYSPACES,
              FIND_NAMESPACES,
              FIND_ONE,
              FIND_RERANKING_PROVIDERS,
              LIST_INDEXES,
              LIST_TABLES,
              LIST_TYPES ->
          READ;
      case DELETE_MANY,
              DELETE_ONE,
              FIND_ONE_AND_DELETE,
              FIND_ONE_AND_REPLACE,
              FIND_ONE_AND_UPDATE,
              INSERT_MANY,
              INSERT_ONE,
              UPDATE_MANY,
              UPDATE_ONE ->
          WRITE;
      case ALTER_TABLE,
              ALTER_TYPE,
              CREATE_COLLECTION,
              CREATE_INDEX,
              CREATE_KEYSPACE,
              CREATE_NAMESPACE,
              CREATE_TABLE,
              CREATE_TEXT_INDEX,
              CREATE_TYPE,
              CREATE_VECTOR_INDEX,
              DELETE_COLLECTION,
              DROP_INDEX,
              DROP_KEYSPACE,
              DROP_NAMESPACE,
              DROP_TABLE,
              DROP_TYPE ->
          ADMIN;
      case BEGIN_OFFLINE_SESSION, END_OFFLINE_SESSION, OFFLINE_GET_STATUS, OFFLINE_INSERT_MANY ->
          throw new IllegalArgumentException("Command is unavailable over MCP: " + command);
    };
  }
}
