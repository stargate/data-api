package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;

/*
 * All the commands that accept {@code SortClause} will have to implement this interface.
 * Will delegate most of the work to {@link SortDefinition} which in turn will delegate
 * to {@link SortClauseBuilder}.
 */
public interface Sortable {
  /** Accessor for the Sort definition in its intermediate JSON form */
  SortDefinition sortDefinition();

  /**
   * Convenience accessor for fully processed SortClause: will convert the intermediate JSON value
   * to a {@link SortClause} instance, including all validation.
   *
   * @param ctx Processing context for the command; used mostly to get the schema object for the
   *     builder
   */
  default SortClause sortClause(CommandContext<?> ctx) {
    SortDefinition def = sortDefinition();
    return (def == null) ? SortClause.empty() : def.toClause(ctx);
  }
}
