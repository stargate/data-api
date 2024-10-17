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
    ALTER_TABLE("alterTable"),
    COUNT_DOCUMENTS("countDocuments"),
    CREATE_COLLECTION("createCollection"),
    CREATE_INDEX("createIndex"),
    CREATE_VECTOR_INDEX("createVectorIndex"),
    CREATE_KEYSPACE("createKeyspace"),
    CREATE_NAMESPACE("createNamespace"),
    CREATE_TABLE("createTable"),
    DELETE_COLLECTION("deleteCollection"),
    DELETE_MANY("deleteMany"),
    DELETE_ONE("deleteOne"),
    DROP_INDEX("dropIndex"),
    DROP_NAMESPACE("dropNamespace"),
    DROP_KEYSPACE("dropKeyspace"),
    DROP_TABLE("dropTable"),
    ESTIMATED_DOCUMENT_COUNT("estimatedDocumentCount"),
    FIND_COLLECTIONS("findCollections"),
    FIND("find"),
    FIND_EMBEDDING_PROVIDERS("findEmbeddingProviders"),
    FIND_NAMESPACES("findNamespaces"),
    FIND_KEYSPACES("findKeyspaces"),
    FIND_ONE_AND_DELETE("findOneAndDelete"),
    FIND_ONE_AND_REPLACE("findOneAndReplace"),
    FIND_ONE_AND_UPDATE("findOneAndUpdate"),
    FIND_ONE("findOne"),
    INSERT_MANY("insertMany"),
    INSERT_ONE("insertOne"),
    LIST_TABLES("listTables"),
    UPDATE_MANY("updateMany"),
    UPDATE_ONE("updateOne"),
    BEGIN_OFFLINE_SESSION("beginOfflineSession"),
    END_OFFLINE_SESSION("endOfflineSession"),
    OFFLINE_GET_STATUS("offlineGetStatus"),
    OFFLINE_INSERT_MANY("offlineInsertMany");

    private final String apiName;

    CommandName(String apiName) {
      this.apiName = apiName;
    }

    public String getApiName() {
      return apiName;
    }
  }
}
