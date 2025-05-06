package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.io.IOException;

/*
 * All the commands that needs SortClause will have to implement this.
 */
public interface Sortable {
  /** Accessor for the filter specification in its intermediate for */
  SortSpec sortSpec();

  default SortClause sortClause(CommandContext<?> ctx) {
    return sortClause(ctx.schemaObject());
  }

  default SortClause sortClause(SchemaObject schema) {
    SortSpec spec = sortSpec();
    try {
      return (spec == null) ? SortClause.empty() : spec.toSortClause(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
