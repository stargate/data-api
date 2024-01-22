package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;

/** Interface for all commands executed against a namespace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateCollectionCommand.class),
  @JsonSubTypes.Type(value = FindCollectionsCommand.class),
  @JsonSubTypes.Type(value = DeleteCollectionCommand.class),
})
public interface NamespaceCommand extends Command {}
