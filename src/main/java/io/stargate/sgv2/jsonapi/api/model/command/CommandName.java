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

  ALTER_TABLE("alterTable", CommandType.DDL, CommandTarget.TABLE),
  COUNT_DOCUMENTS("countDocuments", CommandType.DML, CommandTarget.COLLECTION),
  CREATE_COLLECTION("createCollection", CommandType.DDL, CommandTarget.KEYSPACE),
  CREATE_INDEX("createIndex", CommandType.DDL, CommandTarget.TABLE),
  CREATE_VECTOR_INDEX("createVectorIndex", CommandType.DDL, CommandTarget.TABLE),
  CREATE_KEYSPACE("createKeyspace", CommandType.DDL, CommandTarget.DATABASE),
  CREATE_NAMESPACE("createNamespace", CommandType.DDL, CommandTarget.DATABASE),
  CREATE_TABLE("createTable", CommandType.DDL, CommandTarget.KEYSPACE),
  DELETE_COLLECTION("deleteCollection", CommandType.DDL, CommandTarget.KEYSPACE),
  DELETE_MANY("deleteMany", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  DELETE_ONE("deleteOne", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  DROP_INDEX("dropIndex", CommandType.DDL, CommandTarget.KEYSPACE),
  DROP_NAMESPACE("dropNamespace", CommandType.DDL, CommandTarget.DATABASE),
  DROP_KEYSPACE("dropKeyspace", CommandType.DDL, CommandTarget.DATABASE),
  DROP_TABLE("dropTable", CommandType.DDL, CommandTarget.KEYSPACE),
  ESTIMATED_DOCUMENT_COUNT("estimatedDocumentCount", CommandType.DML, CommandTarget.COLLECTION),
  FIND_COLLECTIONS("findCollections", CommandType.DDL, CommandTarget.KEYSPACE),
  FIND("find", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  FIND_EMBEDDING_PROVIDERS("findEmbeddingProviders", CommandType.ADMIN, CommandTarget.DATABASE),
  FIND_NAMESPACES("findNamespaces", CommandType.DDL, CommandTarget.DATABASE),
  FIND_KEYSPACES("findKeyspaces", CommandType.DDL, CommandTarget.DATABASE),
  FIND_ONE_AND_DELETE("findOneAndDelete", CommandType.DML, CommandTarget.COLLECTION),
  FIND_ONE_AND_REPLACE("findOneAndReplace", CommandType.DML, CommandTarget.COLLECTION),
  FIND_ONE_AND_UPDATE("findOneAndUpdate", CommandType.DML, CommandTarget.COLLECTION),
  FIND_ONE("findOne", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  INSERT_MANY("insertMany", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  INSERT_ONE("insertOne", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  LIST_INDEXES("listIndexes", CommandType.DDL, CommandTarget.TABLE),
  LIST_TABLES("listTables", CommandType.DDL, CommandTarget.KEYSPACE),
  UPDATE_MANY("updateMany", CommandType.DML, CommandTarget.COLLECTION),
  UPDATE_ONE("updateOne", CommandType.DML, CommandTarget.TABLE, CommandTarget.COLLECTION),
  BEGIN_OFFLINE_SESSION("beginOfflineSession", CommandType.DML, CommandTarget.SYSTEM),
  END_OFFLINE_SESSION("endOfflineSession", CommandType.DML, CommandTarget.SYSTEM),
  OFFLINE_GET_STATUS("offlineGetStatus", CommandType.DML, CommandTarget.SYSTEM),
  OFFLINE_INSERT_MANY("offlineInsertMany", CommandType.DML, CommandTarget.SYSTEM);

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
}
