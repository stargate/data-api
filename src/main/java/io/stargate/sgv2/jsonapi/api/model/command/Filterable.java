package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;

/*
 * All the commands that needs FilterClause will have to implement this.
 */
public interface Filterable {
  /** Accessor for the filter specification in its intermediate for */
  FilterSpec filterSpec();

  default FilterClause filterClause(CommandContext<?> ctx) {
    FilterSpec spec = filterSpec();
    return (spec == null) ? null : spec.toFilterClause(ctx);
  }
}
