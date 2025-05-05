package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the deleteMany API {@link Command}.
 *
 * @param filterSpec {@link FilterClause} used to identify documents.
 */
@Schema(
    description =
        "Command that finds documents based on the filter and deletes them from a collection")
@JsonTypeName(CommandName.Names.DELETE_MANY)
public record DeleteManyCommand(
    @Schema(
            description = "Filter clause based on which documents are identified",
            implementation = FilterClause.class)
        @Valid
        @JsonProperty("filter")
        FilterSpec filterSpec)
    implements ModifyCommand, NoOptionsCommand, Filterable {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.DELETE_MANY;
  }
}
