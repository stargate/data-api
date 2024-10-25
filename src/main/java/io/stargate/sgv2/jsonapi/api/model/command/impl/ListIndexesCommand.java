package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available indexes in a table.")
@JsonTypeName("listIndexes")
public record ListIndexesCommand(Options options) implements CollectionCommand {
  public record Options(
      @Schema(
              description = "include table properties.",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          boolean explain) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.LIST_INDEXES;
  }
}
