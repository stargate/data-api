package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that finds a single JSON document from a collection and deletes it. The deleted document is returned")
@JsonTypeName(CommandName.Names.FIND_ONE_AND_DELETE)
public record FindOneAndDeleteCommand(
    @Valid @JsonProperty("filter") FilterClause filterClause,
    @Valid @JsonProperty("sort") SortClause sortClause,
    @JsonProperty("projection") JsonNode projectionDefinition)
    implements ModifyCommand, Filterable, Projectable, Sortable {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_ONE_AND_DELETE;
  }
}
