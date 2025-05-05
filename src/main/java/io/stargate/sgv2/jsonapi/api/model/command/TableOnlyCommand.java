package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListTablesCommand;

/** Interface for all commands executed against a keyspace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CreateTableCommand.class),
  @JsonSubTypes.Type(value = DropIndexCommand.class),
  @JsonSubTypes.Type(value = DropTableCommand.class),
  @JsonSubTypes.Type(value = ListTablesCommand.class)
})
public interface TableOnlyCommand extends KeyspaceCommand {}
