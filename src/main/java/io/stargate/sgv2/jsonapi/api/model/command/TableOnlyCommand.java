package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;

/** Interface for all commands executed against a keyspace. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AlterTypeCommand.class),
  @JsonSubTypes.Type(value = CreateTableCommand.class),
  @JsonSubTypes.Type(value = CreateTypeCommand.class),
  @JsonSubTypes.Type(value = DropIndexCommand.class),
  @JsonSubTypes.Type(value = DropTableCommand.class),
  @JsonSubTypes.Type(value = DropTypeCommand.class),
  @JsonSubTypes.Type(value = ListTablesCommand.class),
  @JsonSubTypes.Type(value = ListTypesCommand.class)
})
public interface TableOnlyCommand extends KeyspaceCommand {}
