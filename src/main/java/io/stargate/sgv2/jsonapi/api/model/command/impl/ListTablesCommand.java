package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionOnlyCommand;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available tables in a namespace.")
@JsonTypeName("listTables")
public record ListTablesCommand(Options options) implements CollectionOnlyCommand {
  public record Options(
      @Schema(
              description = "include table properties.",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          boolean explain) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.LIST_TABLES;
  }
}
