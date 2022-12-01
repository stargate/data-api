package io.stargate.sgv3.docsapi.api.model.command;

import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortClause;

/** Basic interface for all read commands. */
public interface ReadCommand extends Command {

  /**
   * @return Returns the sort clause for this command or <code>null</code> if the sorting is not
   *     defined.
   */
  SortClause sortClause();
}
