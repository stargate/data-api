package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause.Filterable;
import io.stargate.sgv3.docsapi.commands.clauses.OptionsClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause;
import io.stargate.sgv3.docsapi.commands.clauses.ProjectionClause.Projectable;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause;
import io.stargate.sgv3.docsapi.commands.clauses.UpdateClause.Updatable;

/**
 * findOneAndUpdate API command, finds a document and updates it and returns data, either the doc
 * before or after
 */
public class FindOneAndUpdateCommand extends ModifyCommand
    implements Filterable, Updatable, Projectable {

  private final FilterClause filter;
  private final UpdateClause updateDoc;
  public final ProjectionClause projection;

  public final Options options;

  public FindOneAndUpdateCommand(
      FilterClause filter, UpdateClause updateDoc, ProjectionClause projection, Options options) {
    this.filter = filter;
    this.updateDoc = updateDoc;
    this.projection = projection;
    this.options = options;
    options.limit = 1;
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

  @Override
  public ProjectionClause getProjection() {
    return projection;
  }

  /** */
  public static class Options extends OptionsClause {

    public static final Options DEFAULT = new Options(false, ReturnDocumentOption.BEFORE);

    public final boolean upsert;
    public final ReturnDocumentOption returnDocument;
    public int limit = 1;

    public Options(boolean upsert, ReturnDocumentOption returnDocument) {
      this.upsert = upsert;
      this.returnDocument = returnDocument;
    }

    public enum ReturnDocumentOption {
      BEFORE,
      AFTER
    }
  }
}
