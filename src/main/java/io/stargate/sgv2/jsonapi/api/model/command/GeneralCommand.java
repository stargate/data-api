package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;

/**
 * Interface for all general commands, that are not executed against a namespace nor a collection .
 * Note, {@link CreateNamespaceCommand}, {@link DropNamespaceCommand}, {@link FindKeyspacesCommand}
 * are deprecated, support them for backwards-compatibility
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateKeyspaceCommand.class),
  @JsonSubTypes.Type(value = CreateNamespaceCommand.class), // deprecated
  @JsonSubTypes.Type(value = DropKeyspaceCommand.class),
  @JsonSubTypes.Type(value = DropNamespaceCommand.class), // deprecated
  @JsonSubTypes.Type(value = FindEmbeddingProvidersCommand.class),
  @JsonSubTypes.Type(value = FindKeyspacesCommand.class),
  @JsonSubTypes.Type(value = FindNamespacesCommand.class), // deprecated
  @JsonSubTypes.Type(value = FindRerankingProvidersCommand.class),
})
public interface GeneralCommand extends Command {}
