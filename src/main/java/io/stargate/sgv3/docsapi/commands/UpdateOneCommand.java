package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause.Filterable;
import io.stargate.sgv3.docsapi.commands.clauses.OptionsClause;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause.Updatable;

/**
 * updateOne API command, finds a document and updates it - possible doing an upsert to create if no
 * doc matched.
 */
public class UpdateOneCommand extends ModifyCommand implements Filterable, Updatable {

  private final FilterClause filter;
  private final UpdateClause updateDoc;
  public final Options options;

  public UpdateOneCommand(FilterClause filter, UpdateClause updateDoc, Options options) {
    this.filter = filter;
    this.updateDoc = updateDoc;
    this.options = options == null ? Options.DEFAULT : options;
  }

  @Override
  public boolean valid() {
    return false;
  }

  @Override
  public void validate() throws Exception {}

  @Override
  public FilterClause getFilter() {
    return filter;
  }

  @Override
  public UpdateClause getUpdate() {
    return updateDoc;
  }

  /** */
  public static class Options extends OptionsClause {

    public static final Options DEFAULT = new Options(false);

    public final boolean upsert;

    public Options(boolean upsert) {
      this.upsert = upsert;
    }
  }
}
