package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that lists all available indexes in a table.")
@JsonTypeName("listIndexes")
public record ListIndexesCommand(
    @Nullable
        @Schema(
            description = "Options for the `listIndexes` command.",
            type = SchemaType.OBJECT,
            implementation = ListIndexesCommand.Options.class)
        Options options)
    implements CollectionCommand {
  public record Options(
      @Schema(
              description = "Include indexes properties.",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          boolean explain) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.LIST_INDEXES;
  }
}
