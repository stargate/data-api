package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the deleteOne API {@link Command}.
 *
 * @param filterDefinition {@link FilterClause} used to identify a document.
 */
@Schema(description = "Command that finds a single document and deletes it from a collection")
@JsonTypeName(CommandName.Names.DELETE_ONE)
public record DeleteOneCommand(
    @NotNull
        @Schema(
            description = "Filter clause based on which document is identified",
            implementation = FilterClause.class)
        @Valid
        @JsonProperty("filter")
        FilterDefinition filterDefinition,
    @Valid @JsonProperty("sort") SortDefinition sortDefinition)
    implements ModifyCommand, NoOptionsCommand, Filterable, Sortable {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DELETE_ONE;
  }
}
