package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateEmbeddingServiceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindNamespacesCommand;

/**
 * Interface for all general commands, that are not executed against a namespace nor a collection .
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateEmbeddingServiceCommand.class),
  @JsonSubTypes.Type(value = CreateNamespaceCommand.class),
  @JsonSubTypes.Type(value = DropNamespaceCommand.class),
  @JsonSubTypes.Type(value = FindNamespacesCommand.class),
})
public interface GeneralCommand extends Command {}
