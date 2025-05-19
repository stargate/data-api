package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/*
 * All the commands that need {@link SortClause} will have to implement this.
 * Will delegate most of the work to {@link SortSpec} which in turn will delegate
 * to {@link SortClauseBuilder}.
 */
public interface Sortable {
  /** Accessor for the Sort specification in its intermediate for */
  SortDefinition sortDefinition();

  /**
   * Convenience accessor for fully processed SortClause: will convert the intermediate JSON value
   * to a {@link SortClause} instance, including all validation. Delegates to {@link
   * #sortClause(SchemaObject)}.
   *
   * @param ctx Processing context for the command; used to get the schema object for the builder
   */
  default SortClause sortClause(CommandContext<?> ctx) {
    return sortClause(ctx.schemaObject());
  }

  /**
   * Accessor for fully processed {@link SortClause}: will convert the intermediate JSON value to a
   * {@link SortClause} instance, including all validation
   *
   * @param schema Collection or Table for the current command.
   */
  default SortClause sortClause(SchemaObject schema) {
    SortDefinition spec = sortDefinition();
    return (spec == null) ? SortClause.empty() : spec.toSortClause(schema);
  }
}
