package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolver;

/**
 * POJO object (data no behavior) that represents a syntactically and grammatically valid command as
 * defined in the API spec.
 *
 * <p>The behavior about *how* to run a Command is in the {@link CommandResolver}.
 *
 * <p>Commands <b>should not</b> include JSON other than for documents we want to insert. They
 * should represent a translate of the API request into an internal representation. e.g. this
 * insulates from tweaking JSON on the wire protocol, we would only need to modify how we create the
 * command and nothing else.
 *
 * <p>These may be created from parsing the incoming message and could also be created
 * programmatically for internal and testing purposes.
 *
 * <p>Each command should validate itself using the <i>javax.validation</i> framework.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CollectionOnlyCommand.class),
  @JsonSubTypes.Type(value = TableOnlyCommand.class),
  @JsonSubTypes.Type(value = GeneralCommand.class),
  @JsonSubTypes.Type(value = CollectionCommand.class),
})
public interface Command {

  /**
   * commandName that refers to the api command name
   *
   * <p>e.g. FindKeyspacesCommand publicCommandName -> findKeyspaces. CreateCollectionCommand
   * publicCommandName -> createCollection
   */
  CommandName commandName();

  /** Enum class for API command name. This is what user uses for command json body. */
  enum CommandName {
    ALTER_TABLE("alterTable", CommandType.DDL),
    COUNT_DOCUMENTS("countDocuments", CommandType.DML),
    CREATE_COLLECTION("createCollection", CommandType.DDL),
    CREATE_INDEX("createIndex", CommandType.DDL),
    CREATE_VECTOR_INDEX("createVectorIndex", CommandType.DDL),
    CREATE_KEYSPACE("createKeyspace", CommandType.DDL),
    CREATE_NAMESPACE("createNamespace", CommandType.DDL),
    CREATE_TABLE("createTable", CommandType.DDL),
    DELETE_COLLECTION("deleteCollection", CommandType.DDL),
    DELETE_MANY("deleteMany", CommandType.DML),
    DELETE_ONE("deleteOne", CommandType.DML),
    DROP_INDEX("dropIndex", CommandType.DML),
    DROP_NAMESPACE("dropNamespace", CommandType.DDL),
    DROP_KEYSPACE("dropKeyspace", CommandType.DDL),
    DROP_TABLE("dropTable", CommandType.DDL),
    ESTIMATED_DOCUMENT_COUNT("estimatedDocumentCount", CommandType.DML),
    FIND_COLLECTIONS("findCollections", CommandType.DDL),
    FIND("find", CommandType.DML),
    FIND_EMBEDDING_PROVIDERS("findEmbeddingProviders", CommandType.DML),
    FIND_NAMESPACES("findNamespaces", CommandType.DDL),
    FIND_KEYSPACES("findKeyspaces", CommandType.DDL),
    FIND_ONE_AND_DELETE("findOneAndDelete", CommandType.DML),
    FIND_ONE_AND_REPLACE("findOneAndReplace", CommandType.DML),
    FIND_ONE_AND_UPDATE("findOneAndUpdate", CommandType.DML),
    FIND_ONE("findOne", CommandType.DML),
    INSERT_MANY("insertMany", CommandType.DML),
    INSERT_ONE("insertOne", CommandType.DML),
    LIST_INDEXES("listIndexes", CommandType.DDL),
    LIST_TABLES("listTables", CommandType.DDL),
    UPDATE_MANY("updateMany", CommandType.DML),
    UPDATE_ONE("updateOne", CommandType.DML),
    BEGIN_OFFLINE_SESSION("beginOfflineSession", CommandType.DML),
    END_OFFLINE_SESSION("endOfflineSession", CommandType.DML),
    OFFLINE_GET_STATUS("offlineGetStatus", CommandType.DML),
    OFFLINE_INSERT_MANY("offlineInsertMany", CommandType.DML);

    private final String apiName;
    private final CommandType commandType;

    CommandName(String apiName, CommandType commandType) {
      this.apiName = apiName;
      this.commandType = commandType;
    }

    public String getApiName() {
      return apiName;
    }

    public CommandType getCommandType() {
      return commandType;
    }
  }

  /** Enum class for command types. This is used to categorize commands into DDL and DML. */
  enum CommandType {
    DDL,
    DML;
  }
}
