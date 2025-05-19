package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;

/*
 * All the commands that needs FilterClause will have to implement this.
 */
public interface Filterable {
  /** Accessor for the filter specification in its intermediate for */
  FilterDefinition filterDefinition();

  default FilterClause filterClause(CommandContext<?> ctx) {
    FilterDefinition spec = filterDefinition();
    return (spec == null) ? FilterClause.empty() : spec.toFilterClause(ctx);
  }
}
