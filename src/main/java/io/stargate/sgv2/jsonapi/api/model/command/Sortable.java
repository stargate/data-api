package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;

/*
 * All the commands that needs SortClause will have to implement this.
 */
public interface Sortable {
  SortClause sortClause();
}
