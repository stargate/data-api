package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.RenameTypeFieldsDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import jakarta.validation.constraints.NotEmpty;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * AlterType command to rename fields or add fields in a user-defined type (UDT). This command does
 * not support options like IF NOT EXISTS or IF EXISTS, as these are not available in Astra, even
 * though they are supported in Cassandra 5.
 */
@Schema(description = "Command that alters a user defined type.")
@JsonTypeName(CommandName.Names.ALTER_TYPE)
public record AlterTypeCommand(
    @NotEmpty @Schema(description = "Required name of the type") String name,
    @Nullable @Schema(description = "Operation to rename fields") RenameTypeFieldsDesc rename,
    @Nullable @Schema(description = "Operation to add fields") TypeDefinitionDesc add)
    implements TableOnlyCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.ALTER_TYPE;
  }
}
