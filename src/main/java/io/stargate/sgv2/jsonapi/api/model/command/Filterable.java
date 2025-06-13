package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;

/*
 * All the commands that accept {@code FilterClause} will have to implement this interface.
 */
public interface Filterable {
  /** Accessor for the filter definition in its intermediate JSON form */
  FilterDefinition filterDefinition();

  default FilterClause filterClause(CommandContext<?> ctx) {
    FilterDefinition def = filterDefinition();
    return (def == null) ? FilterClause.empty() : def.build(ctx);
  }
}
