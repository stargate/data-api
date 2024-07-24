package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;

/** Interface for all commands executed against a keyspace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({@JsonSubTypes.Type(value = CreateTableCommand.class)})
public interface KeyspaceCommand extends SchemaCommand {}
