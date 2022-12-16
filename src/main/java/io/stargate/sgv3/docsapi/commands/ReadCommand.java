package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;

/**
 * Base for any commands that read, like find and findOne
 *
 * <p>Can add common behavior like all reading has a {@link ProjectionClause}
 */
public abstract class ReadCommand extends Command
    implements ProjectionClause.Projectable, FilterClause.Filterable {

  protected final FilterClause filter;
  protected final ProjectionClause projection;

  protected ReadCommand(FilterClause filter, ProjectionClause projection) {
    this.filter = filter;
    this.projection = projection;
  }

  @Override
  public FilterClause getFilter() {
    return filter;
  }

  @Override
  public ProjectionClause getProjection() {
    return projection;
  }
}
