package io.stargate.sgv3.docsapi.api.model.command;

import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterClause;

/*
 * All the commands that needs FilterClause will have to implement this.
 */
public interface Filterable {
  FilterClause filterClause();
}
