package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;

/** Interface for all commands executed against a collection in a namespace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CountDocumentsCommand.class),
  @JsonSubTypes.Type(value = DeleteOneCommand.class),
  @JsonSubTypes.Type(value = DeleteManyCommand.class),
  @JsonSubTypes.Type(value = FindCommand.class),
  @JsonSubTypes.Type(value = FindOneCommand.class),
  @JsonSubTypes.Type(value = FindOneAndDeleteCommand.class),
  @JsonSubTypes.Type(value = FindOneAndReplaceCommand.class),
  @JsonSubTypes.Type(value = FindOneAndUpdateCommand.class),
  @JsonSubTypes.Type(value = InsertOneCommand.class),
  @JsonSubTypes.Type(value = InsertManyCommand.class),
  @JsonSubTypes.Type(value = UpdateManyCommand.class),
  @JsonSubTypes.Type(value = UpdateOneCommand.class),
})
public interface CollectionCommand extends Command {}
