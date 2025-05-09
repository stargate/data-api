package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and updates the value provided in the update clause.")
@JsonTypeName(CommandName.Names.UPDATE_ONE)
public record UpdateOneCommand(
    @Valid @JsonProperty("filter") FilterSpec filterSpec,
    @NotNull @Valid @JsonProperty("update") UpdateClause updateClause,
    @Valid @JsonProperty("sort") SortSpec sortSpec,
    @Nullable Options options)
    implements ReadCommand, Filterable, Sortable, Updatable {

  @Schema(name = "UpdateOneCommand.Options", description = "Options for updating a document.")
  public record Options(
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply the `update` clause and all equality filters to the empty document.",
              defaultValue = "false")
          boolean upsert) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.UPDATE_ONE;
  }
}
