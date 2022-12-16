package io.stargate.sgv3.docsapi.commands.clauses;

import io.stargate.sgv3.docsapi.commands.clauses.update.UpdateClauseOperation;
import java.util.List;

public class UpdateClause extends Clause {

  public final List<UpdateClauseOperation> operations;

  public UpdateClause(List<UpdateClauseOperation> operations) {
    this.operations = operations;
  }

  public interface Updatable {
    UpdateClause getUpdate();
  }
}
