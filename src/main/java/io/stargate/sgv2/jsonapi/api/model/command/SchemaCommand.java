package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Interface for all commands executed for schema related command for both Collection and Table. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = NamespaceCommand.class),
  @JsonSubTypes.Type(value = KeyspaceCommand.class)
})
public interface SchemaCommand extends Command {}
