package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds documents from a collection and updates it with the values provided in the update clause.")
@JsonTypeName(CommandName.Names.UPDATE_MANY)
public record UpdateManyCommand(
    @Valid @JsonProperty("filter") FilterDefinition filterDefinition,
    @NotNull @Valid @JsonProperty("update") UpdateClause updateClause,
    @Nullable Options options)
    implements ReadCommand, Filterable, Updatable {

  @Schema(name = "UpdateManyCommand.Options", description = "Options for updating many documents.")
  public record Options(
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply the `update` clause and all equality filters to the empty document.",
              defaultValue = "false")
          boolean upsert,
      @Nullable
          @Schema(
              description = "Next page state for pagination.",
              type = SchemaType.STRING,
              implementation = String.class)
          @JsonProperty("pageState")
          @JsonAlias("pagingState") // old name, 1.0.0-BETA-3 and prior
          String pageState) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.UPDATE_MANY;
  }
}
