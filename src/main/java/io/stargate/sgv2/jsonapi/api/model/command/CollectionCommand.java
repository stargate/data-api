package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;

/** Interface for all commands executed against a collection in a keyspace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CountDocumentsCommand.class), // not supported in table
  @JsonSubTypes.Type(value = DeleteManyCommand.class),
  @JsonSubTypes.Type(value = DeleteOneCommand.class),
  @JsonSubTypes.Type(value = EstimatedDocumentCountCommand.class), // not supported in table
  @JsonSubTypes.Type(value = FindAndRerankCommand.class), // not supported in table
  @JsonSubTypes.Type(value = FindCommand.class), //
  @JsonSubTypes.Type(value = FindOneAndDeleteCommand.class), // not supported in table
  @JsonSubTypes.Type(value = FindOneAndReplaceCommand.class), // not supported in table
  @JsonSubTypes.Type(value = FindOneAndUpdateCommand.class), // not supported in table
  @JsonSubTypes.Type(value = FindOneCommand.class), //
  @JsonSubTypes.Type(value = InsertManyCommand.class), //
  @JsonSubTypes.Type(value = InsertOneCommand.class), //
  @JsonSubTypes.Type(value = UpdateManyCommand.class), // not supported in table
  @JsonSubTypes.Type(value = UpdateOneCommand.class), //
  // We have only collection resource that is used for API Tables
  @JsonSubTypes.Type(value = AlterTableCommand.class), //
  @JsonSubTypes.Type(value = CreateIndexCommand.class), //
  @JsonSubTypes.Type(value = CreateTextIndexCommand.class), //
  @JsonSubTypes.Type(value = CreateVectorIndexCommand.class), //
  @JsonSubTypes.Type(value = ListIndexesCommand.class), //
  // The commands supported by tables are:
  //  deleteMany, deleteOne,
  // updateOne.
})
public interface CollectionCommand extends Command {}
