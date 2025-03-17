package io.stargate.sgv2.jsonapi.api.model.command;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The canonical list of commands the API support, any command name not listed here is not a
 * command.
 */
public enum CommandName {
  // TODO: the embedding providers and offline commands should not be DML, they are not changing
  // data
  // they should not be DDL, they are not changing schema, we should add an CommandType.ADMIN for
  // them ?

  ALTER_TABLE(Names.ALTER_TABLE_NAME, CommandType.DDL, CommandTarget.TABLE),
  COUNT_DOCUMENTS(Names.COUNT_DOCUMENTS, CommandType.DML, CommandTarget.COLLECTION),
  CREATE_COLLECTION(Names.CREATE_COLLECTION, CommandType.DDL, CommandTarget.KEYSPACE),
  CREATE_INDEX(Names.CREATE_INDEX, CommandType.DDL, CommandTarget.TABLE),
  CREATE_VECTOR_INDEX(Names.CREATE_VECTOR_INDEX, CommandType.DDL, CommandTarget.TABLE),
  CREATE_KEYSPACE(Names.CREATE_KEYSPACE, CommandType.DDL, CommandTarget.DATABASE),
  CREATE_NAMESPACE(Names.CREATE_NAMESPACE, CommandType.DDL, CommandTarget.DATABASE),
  CREATE_TABLE(Names.CREATE_TABLE, CommandType.DDL, CommandTarget.KEYSPACE),
  DELETE_COLLECTION(Names.DELETE_COLLECTION, CommandType.DDL, CommandTarget.KEYSPACE),
  DELETE_MANY(Names.DELETE_MANY, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  DELETE_ONE(Names.DELETE_ONE, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  DROP_INDEX(Names.DROP_INDEX, CommandType.DDL, CommandTarget.KEYSPACE),
  DROP_NAMESPACE(Names.DROP_NAMESPACE, CommandType.DDL, CommandTarget.DATABASE),
  DROP_KEYSPACE(Names.DROP_KEYSPACE, CommandType.DDL, CommandTarget.DATABASE),
  DROP_TABLE(Names.DROP_TABLE, CommandType.DDL, CommandTarget.KEYSPACE),
  ESTIMATED_DOCUMENT_COUNT(
      Names.ESTIMATED_DOCUMENT_COUNT, CommandType.DML, CommandTarget.COLLECTION),
  FIND(Names.FIND, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  FIND_COLLECTIONS(Names.FIND_COLLECTIONS, CommandType.DDL, CommandTarget.KEYSPACE),
  FIND_EMBEDDING_PROVIDERS(
      Names.FIND_EMBEDDING_PROVIDERS, CommandType.ADMIN, CommandTarget.DATABASE),
  FIND_RERANKING_PROVIDERS(
      Names.FIND_RERANKING_PROVIDERS, CommandType.ADMIN, CommandTarget.DATABASE),
  FIND_NAMESPACES(Names.FIND_NAMESPACES, CommandType.DDL, CommandTarget.DATABASE),
  FIND_KEYSPACES(Names.FIND_KEYSPACES, CommandType.DDL, CommandTarget.DATABASE),
  FIND_ONE(Names.FIND_ONE, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  FIND_ONE_AND_DELETE(Names.FIND_ONE_AND_DELETE, CommandType.DML, CommandTarget.COLLECTION),
  FIND_ONE_AND_REPLACE(Names.FIND_ONE_AND_REPLACE, CommandType.DML, CommandTarget.COLLECTION),
  FIND_ONE_AND_UPDATE(Names.FIND_ONE_AND_UPDATE, CommandType.DML, CommandTarget.COLLECTION),
  INSERT_MANY(Names.INSERT_MANY, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  INSERT_ONE(Names.INSERT_ONE, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  LIST_INDEXES(Names.LIST_INDEXES, CommandType.DDL, CommandTarget.TABLE),
  LIST_TABLES(Names.LIST_TABLES, CommandType.DDL, CommandTarget.KEYSPACE),
  UPDATE_MANY(Names.UPDATE_MANY, CommandType.DML, CommandTarget.COLLECTION),
  UPDATE_ONE(Names.UPDATE_ONE, CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  BEGIN_OFFLINE_SESSION(Names.BEGIN_OFFLINE_SESSION, CommandType.DML, CommandTarget.SYSTEM),
  END_OFFLINE_SESSION(Names.END_OFFLINE_SESSION, CommandType.DML, CommandTarget.SYSTEM),
  OFFLINE_GET_STATUS(Names.OFFLINE_GET_STATUS, CommandType.DML, CommandTarget.SYSTEM),
  OFFLINE_INSERT_MANY(Names.OFFLINE_INSERT_MANY, CommandType.DML, CommandTarget.SYSTEM);

  private final String apiName;
  private final CommandType commandType;
  private Set<CommandTarget> targets;

  CommandName(String apiName, CommandType commandType, CommandTarget... targets) {
    this.apiName = apiName;
    this.commandType = commandType;
    this.targets = Set.of(targets);
  }

  /**
   * The name to use for this command in the API, i.e. what we look for when a command is sent.
   *
   * @return The name of the command in the API.
   */
  public String getApiName() {
    return apiName;
  }

  public CommandType getCommandType() {
    return commandType;
  }

  /**
   * The set of targets this command can be used on, some commands support both tables and
   * collections.
   *
   * @return Set of the targets this command can be used on.
   */
  public Set<CommandTarget> getTargets() {
    return targets;
  }

  /**
   * Filter the list of command names.
   *
   * @param target The target to filter by, i.e. get all the commands that work with {@link
   *     CommandTarget#TABLE}.
   * @return List of command names that work with the target.
   */
  public static List<CommandName> filterByTarget(CommandTarget target) {
    return Stream.of(values())
        .filter(commandName -> commandName.getTargets().contains(target))
        .collect(Collectors.toList());
  }

  public interface Names {
    String ALTER_TABLE_NAME = "alterTable";
    String COUNT_DOCUMENTS = "countDocuments";
    String CREATE_COLLECTION = "createCollection";
    String CREATE_INDEX = "createIndex";
    String CREATE_VECTOR_INDEX = "createVectorIndex";
    String CREATE_KEYSPACE = "createKeyspace";
    String CREATE_NAMESPACE = "createNamespace";
    String CREATE_TABLE = "createTable";
    String DELETE_COLLECTION = "deleteCollection";
    String DELETE_MANY = "deleteMany";
    String DELETE_ONE = "deleteOne";
    String DROP_INDEX = "dropIndex";
    String DROP_NAMESPACE = "dropNamespace";
    String DROP_KEYSPACE = "dropKeyspace";
    String DROP_TABLE = "dropTable";
    String ESTIMATED_DOCUMENT_COUNT = "estimatedDocumentCount";
    String FIND = "find";
    String FIND_COLLECTIONS = "findCollections";
    String FIND_EMBEDDING_PROVIDERS = "findEmbeddingProviders";
    String FIND_RERANKING_PROVIDERS = "findRerankingProviders";
    String FIND_NAMESPACES = "findNamespaces";
    String FIND_KEYSPACES = "findKeyspaces";
    String FIND_ONE = "findOne";
    String FIND_ONE_AND_DELETE = "findOneAndDelete";
    String FIND_ONE_AND_REPLACE = "findOneAndReplace";
    String FIND_ONE_AND_UPDATE = "findOneAndUpdate";
    String INSERT_MANY = "insertMany";
    String INSERT_ONE = "insertOne";
    String LIST_INDEXES = "listIndexes";
    String LIST_TABLES = "listTables";
    String UPDATE_MANY = "updateMany";
    String UPDATE_ONE = "updateOne";
    String BEGIN_OFFLINE_SESSION = "beginOfflineSession";
    String END_OFFLINE_SESSION = "endOfflineSession";
    String OFFLINE_GET_STATUS = "offlineGetStatus";
    String OFFLINE_INSERT_MANY = "offlineInsertMany";
  }
}
