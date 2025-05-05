package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDefinitionDesc;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates an API Table.")
@JsonTypeName(CommandName.Names.CREATE_TABLE)
public record CreateTableCommand(
    @Schema(description = "Required name of the new Table") String name,
    @Valid @NotNull @Schema(description = "Table definition") TableDefinitionDesc definition,
    @Valid
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Nullable
        @Schema(
            description = "Configuration options for the collection",
            type = SchemaType.OBJECT,
            implementation = Options.class)
        Options options)
    implements TableOnlyCommand {

  public record Options(
      @Nullable
          @Schema(
              description = "Flag to ignore if table already exists",
              defaultValue = "false",
              type = SchemaType.BOOLEAN,
              implementation = Boolean.class)
          Boolean ifNotExists) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_TABLE;
  }
}
