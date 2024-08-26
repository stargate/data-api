package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command for dropping a table.
 *
 * @param name Name of the table
 */
// TODO, hide table feature detail before it goes public
// @Schema(description = "Command that drops a table if one exists.")
@JsonTypeName("dropTable")
public record DropTableCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the table")
        String name)
    implements TableOnlyCommand, NoOptionsCommand {}
