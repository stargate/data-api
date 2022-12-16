package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.OptionsClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import io.stargate.sgv3.docsapi.commands.clauses.SortClause;

/**
 * Internal representation of the findOne API {@link Command}.
 *
 * <p>As a Command it should represent the request without using JSON.
 */
public class FindOneCommand extends ReadCommand {

  // These are the clauses this command supports
  public final SortClause sort;
  public final Options options;

  public FindOneCommand(
      FilterClause filter, ProjectionClause projection, SortClause sort, Options options) {
    super(filter, projection);
    this.options = options;
    this.sort = sort;
  }

  @Override
  public boolean valid() {
    return false;
  }

  @Override
  public void validate() throws Exception {}

  /** TODO placeholder for command options, findOne does not have any special ones (yet) :| */
  public static class Options extends OptionsClause {}
}
