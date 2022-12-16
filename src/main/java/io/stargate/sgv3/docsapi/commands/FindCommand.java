package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.OptionsClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import io.stargate.sgv3.docsapi.commands.clauses.SortClause;

/**
 * Internal representation of the find API {@link Command}.
 *
 * <p>As a Command it should represent the request without using JSON.
 */
public class FindCommand extends ReadCommand {

  // These are the clauses this command supports
  public final SortClause sort;
  public final Options options;

  public FindCommand(
      FilterClause filter, ProjectionClause projection, SortClause sort, Options options) {
    super(filter, projection);
    this.sort = sort;
    this.options = options;
  }

  public FindCommand() {
    this(new FilterClause(), ProjectionClause.ALL, null, new Options());
  }

  @Override
  public boolean valid() {
    return true;
  }

  @Override
  public void validate() throws Exception {}

  public Options getOptions() {
    return options;
  }

  /** TODO placeholder for command options, find does not have any special ones (yet) :| */
  public static class Options extends OptionsClause {
    public String pagingState;
    public int limit;

    public Options() {
      this(null, 0);
    }

    public Options(String pagingState, int limit) {
      this.pagingState = pagingState;
      this.limit = limit;
    }
  }
}
