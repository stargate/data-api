package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available tables in a keyspace.")
@JsonTypeName(CommandName.Names.LIST_TABLES)
public record ListTablesCommand(
    @Nullable
        @Schema(
            description = "Options for the `listTables` command.",
            type = SchemaType.OBJECT,
            implementation = ListTablesCommand.Options.class)
        Options options)
    implements TableOnlyCommand {
  public record Options(
      @Schema(
              description = "Include table properties.",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          boolean explain) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.LIST_TABLES;
  }
}
