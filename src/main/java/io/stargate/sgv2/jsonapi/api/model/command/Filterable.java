package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;

/*
 * All the commands that needs FilterClause will have to implement this.
 */
public interface Filterable {
  FilterSpec filterClause();
}
