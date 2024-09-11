package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnDataType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKey;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

// TODO, hide table feature detail before it goes public,
// https://github.com/stargate/data-api/pull/1360
// @Schema(description = "Command that creates an api table.")
@JsonTypeName("createTable")
public record CreateTableCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the table")
        String name,
    @Valid @NotNull @Schema(description = "Table definition") Definition definition)
    implements TableOnlyCommand {
  public record Definition(
      @Valid
          @Schema(description = "API table columns definitions", type = SchemaType.OBJECT)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          Map<String, ColumnDataType> columns,
      @Valid
          @Schema(
              description = "Primary key definition for the table",
              anyOf = {String.class, PrimaryKey.class})
          @JsonInclude(JsonInclude.Include.NON_NULL)
          PrimaryKey primaryKey) {}

  /** {@inheritDoc} */
  @Override
  public PublicCommandName publicCommandName() {
    return PublicCommandName.createTable;
  }
}
