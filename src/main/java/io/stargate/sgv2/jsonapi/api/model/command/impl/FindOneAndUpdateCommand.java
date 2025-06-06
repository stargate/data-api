package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and updates the value provided in the update clause.")
@JsonTypeName(CommandName.Names.FIND_ONE_AND_UPDATE)
public record FindOneAndUpdateCommand(
    @Valid @JsonProperty("filter") FilterDefinition filterDefinition,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") SortDefinition sortDefinition,
    @NotNull @Valid @JsonProperty("update") UpdateClause updateClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable, Sortable, Updatable {

  @Schema(
      name = "FindOneAndUpdateCommand.Options",
      description = "Options for `findOneAndUpdate` command.")
  public record Options(
      @Nullable
          @Pattern(
              regexp = "(after|before)",
              message = "returnDocument value can only be 'before' or 'after'")
          @Schema(
              description =
                  "Specifies which document to perform the projection on. If `before` the projection is performed on the document before the update is applied, if `after` the document projection is from the document after the update.",
              defaultValue = "before")
          String returnDocument,
      @Schema(
              description =
                  "When `true`, if no documents match the `filter` clause the command will create a new _empty_ document and apply the `update` clause and all equality filters to the empty document.",
              defaultValue = "false")
          boolean upsert) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_ONE_AND_UPDATE;
  }
}
