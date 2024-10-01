package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Interface for all commands executed for schema related command for both Collection and Table. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CollectionOnlyCommand.class),
  @JsonSubTypes.Type(value = TableOnlyCommand.class)
})
public interface KeyspaceCommand extends Command {}
